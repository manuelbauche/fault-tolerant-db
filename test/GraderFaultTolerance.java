import com.datastax.driver.core.ResultSet;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.utils.Util;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runners.MethodSorters;
import server.faulttolerance.MyDBReplicableAppGP;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The tests in this class test fault tolerance.
 * <p>
 * You can also replace "extends GraderCommonSetup" below with "extends
 * GraderConsistency" if you wish to run tests for both simple (non-fault-prone)
 * replication and fault-tolerance sequentially in one fell swoop.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GraderFaultTolerance extends GraderCommonSetup {

	// True if Gigapaxos being used, false if Zookeeper or anything else.
	public static final boolean GIGAPAXOS_MODE = false;

	/**
	 * Maximum permitted size of any collection that is used to maintain
	 * request-specific state, i.e., you can not maintain state for more than
	 * MAX_LOG_SIZE requests (in memory or on disk). This constraint exists to
	 * ensure that your logs don't grow unbounded, which forces
	 * checkpointing to
	 * be implemented.
	 */
	public static final int MAX_LOG_SIZE = 400;

	/**
	 * Set this string as need to specify the path for cassandra's cqlsh
	 * command, i.e., you can invoke cqlsh as CASSANDRA_HOME_PATH/bin/cqlsh
	 */
	public static final String
			CASSANDRA_HOME_PATH="/Users/arun/apache-cassandra-3.11.11";

	@BeforeClass
	public static void setupFT() throws IOException, InterruptedException {
		if (GIGAPAXOS_MODE) {
			// start gigapaxos servers
			setupGP();
			System.out.println("\nWaiting (" + (PER_SERVER_BOOTSTRAP_TIME *
					(servers.length)) / 1000 + " seconds) for gigapaxos " +
					"servers" + " " + "to " + "start" + " ");
			// sleep to allow servers to bootup
			Thread.sleep(PER_SERVER_BOOTSTRAP_TIME * (servers.length));
		}
		Thread.sleep(SLEEP*servers.length);
	}

	private static Set<ReconfigurableNode> gpServers = new
			HashSet<ReconfigurableNode>();

	private static void setupGP() throws IOException, InterruptedException {
		System.setProperty("gigapaxosConfig", "conf/gigapaxos.properties");
		System.setProperty("java.util.logging.config.file", "logging" + "" +
				"" + ".properties");
		if (DEFER_SERVER_CREATION_TO_CHILD) {
			// Need to create tables before gigapaxos servers wake up as they
			// are expected to issue DB requests while rolling forward from the
			// most recent checkpoint during recovery
			createEmptyTables();
			int i = 0;
			for (String node : servers) {
				Set<ReconfigurableNode> created = (ReconfigurableNode.main1
						(Arrays.asList(node, "start", node).toArray(new
								String[0])));
				for (ReconfigurableNode createdElement : created)
					gpServers.add(createdElement);
				System.out.println("Started node " + node);
			}
		}
	}


	private static String getCommand(String cmd) {
		return !GIGAPAXOS_MODE ? cmd : new RequestPacket(cmd, false)
				.putPaxosID((STUDENT_TESTING_MODE ? MyDBReplicableAppGP.class
						.getSimpleName() : "AVDBReplicableAppGP") + "0", 0)
				.toString();
	}

	private static InetSocketAddress getAddress(String server) {
		return !GIGAPAXOS_MODE ? serverMap.get(server) : new InetSocketAddress
				(serverMap.get(server).getAddress(), ReconfigurationConfig
						.getClientFacingPort(serverMap.get(server).getPort()));
	}

	/**
	 * Issue a request to one server and check that it executed successfully on
	 * all servers in graceful (failure-free) scenarios. This test should pass
	 * even with the simple non-fault-tolerant replicated server and is simply
	 * here for making sure that your fault-tolerant server works as
	 * expected at
	 * least in failure-free scenarios.
	 * <p>
	 * Note that the {@link GraderFaultTolerance#getCommand(String)} method
	 * will
	 * always send requests by wrapping them up into {@link RequestPacket}
	 * packet type for gigapaxos mode, so no special packet type definitions
	 * are
	 * needed.
	 */
	@Test
	public void test31_GracefulExecutionSingleRequest() throws IOException,
			InterruptedException {

		int key = ThreadLocalRandom.current().nextInt();

		String server = servers[0];

		// single update after insert
		client.send(serverMap.get(server), getCommand(insertRecordIntoTableCmd
				(key, DEFAULT_TABLE_NAME)));
		client.send(serverMap.get(server), getCommand(updateRecordOfTableCmd
				(key, DEFAULT_TABLE_NAME)));
		Thread.sleep(SLEEP);

		verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
	}

	/**
	 * Graceful execution as above but with multiple requests to single server.
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void test32_GracefulExecutionMultipleRequestsSingleServer() throws
			IOException, InterruptedException {

		int key = ThreadLocalRandom.current().nextInt();

		// random server
		String server = servers[Math.abs(key % servers.length)];
		client.send(serverMap.get(server), getCommand(insertRecordIntoTableCmd
				(key, DEFAULT_TABLE_NAME)));


		// number of requests is arbitrary
		for (int i = 0; i < servers.length * 2; i++) {
			client.send(serverMap.get(server), getCommand
					(updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME)));
		}
		Thread.sleep(SLEEP);

		verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
	}

	@Test
	public void test33_GracefulExecutionMultipleRequestsToMultipleServers()
			throws IOException, InterruptedException {

		int key = ThreadLocalRandom.current().nextInt();

		// random server
		String server = servers[Math.abs(key % servers.length)];
		client.send(serverMap.get(server), getCommand(insertRecordIntoTableCmd
				(key, DEFAULT_TABLE_NAME)));


		// number of requests is arbitrary
		for (int i = 0; i < servers.length * 2; i++) {
			client.send(getRandomServerAddr(), getCommand
					(updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME)));
		}
		Thread.sleep(SLEEP);

		verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
	}


	private static Set<String> crashed = new HashSet<String>();

	/**
	 * Crash one random server, issue one request, check successful execution
	 * across alive servers.
	 */
	@Test
	public void test34_SingleServerCrash() throws IOException,
			InterruptedException {

		int key = ThreadLocalRandom.current().nextInt();

		String victim = ServerFailureRecoveryManager.killRandomServer();
		crashed.add(victim);
		String server = (String) Util.getRandomOtherThan(serverMap.keySet(),
				victim);

		System.out.println("Sending command to " + server + "; crashed=" +
				crashed);

		// We send requests slowly in a loop until the first one succeeds so
		// that we can be sure that the key record got created and subsequent
		// value insertions will succeed. Upon a crash, if the crashed server
		// was the coordinator, some requests may intermittently fail until a
		// another node realizes the state of affairs. and decides to take over
		// as coordinator.
		do {
			client.send(serverMap.get(server), getCommand
					(insertRecordIntoTableCmd(key, DEFAULT_TABLE_NAME)));
			Thread.sleep(SLEEP);
		} while (serverMap.size() > crashed.size() * 2 //majority
				&& verifyInserted(key, server));

		for (int i = 0; i < servers.length * 2; i++) {
			client.send(serverMap.get((String) Util.getRandomOtherThan
					(serverMap.keySet(), crashed)), getCommand
					(updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME)));
		}
		Thread.sleep(SLEEP);

		verifyOrderConsistent(DEFAULT_TABLE_NAME, key, crashed,
				// nonempty only if majority availability
				serverMap.size() <= 2 * crashed.size());
	}


	/**
	 * Keep the same random server crashed as in the previous test, issue
	 * multiple requests to different remaining servers, check successful
	 * execution across alive servers.
	 */
	@Test
	public void test35_TwoServerCrash() throws IOException,
			InterruptedException {

		int key = ThreadLocalRandom.current().nextInt();

		String victim1 = crashed.iterator().next();
		String victim2 = (String) Util.getRandomOtherThan(serverMap.keySet(),
				victim1);
		ServerFailureRecoveryManager.killServer(victim2);
		crashed.add(victim2);


		String server = (String) Util.getRandomOtherThan(serverMap.keySet(),
				crashed);
		Assert.assertTrue(!server.equals(crashed.iterator().next()));
		System.out.println("Sending request to " + server + "; " + "crashed="
				+ crashed);

		do {
			client.send(serverMap.get(server), getCommand
					(insertRecordIntoTableCmd(key, DEFAULT_TABLE_NAME)));
			Thread.sleep(SLEEP);
		} while (serverMap.size() > crashed.size() * 2 //majority
				&& verifyInserted(key, server));

		for (int i = 0; i < servers.length * 3; i++) {
			client.send(serverMap.get((String) Util.getRandomOtherThan
					(serverMap.keySet(), crashed)), getCommand
					(updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME)));
		}
		Thread.sleep(SLEEP);

		verifyOrderConsistent(DEFAULT_TABLE_NAME, key, crashed,
				// can be empty (no liveness) if less than majority up
				serverMap.size() <= 2 * crashed.size());
	}

	/**
	 * Recovers one of the crashed servers.
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void test36_OneServerRecoveryMultipleRequests() throws IOException,
			InterruptedException {
		String first = crashed.iterator().next();
		ServerFailureRecoveryManager.recoverServer(first);
		Thread.sleep(PER_SERVER_BOOTSTRAP_TIME * servers.length);
		crashed.remove(first);

		int key = ThreadLocalRandom.current().nextInt();

		// No need to wait or sleep here as a recovering server shouldn't be
		// a reason for a request to be lost.
		client.send(serverMap.get((String) Util.getRandomOtherThan(serverMap
				.keySet(), crashed)), getCommand(insertRecordIntoTableCmd(key,
				DEFAULT_TABLE_NAME)));

		String cmd = null;
		for (int i = 0; i < servers.length * 3; i++) {
			client.send(serverMap.get((String) Util.getRandomOtherThan
					(serverMap.keySet(), crashed)), cmd = getCommand
					(updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME)));
		}
		Thread.sleep(SLEEP);

		verifyOrderConsistent(DEFAULT_TABLE_NAME, key, crashed,
				// can be empty (no liveness) if less than majority up
				serverMap.size() <= 2 * crashed.size());
	}

	private static Integer fixedKeyKnownToExist = null;

	/**
	 * Recovers the second crashed server. Equivalent to running the previous
	 * test a second time.
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void test37_TwoServerRecoveryMultipleRequests() throws IOException,
			InterruptedException {
		String first = crashed.iterator().next();
		ServerFailureRecoveryManager.recoverServer(first);
		Thread.sleep(PER_SERVER_BOOTSTRAP_TIME * servers.length);
		crashed.remove(first);

		int key = (fixedKeyKnownToExist = ThreadLocalRandom.current().nextInt
				());
		client.send(serverMap.get((String) Util.getRandomOtherThan(serverMap
				.keySet(), crashed)), getCommand(insertRecordIntoTableCmd(key,
				DEFAULT_TABLE_NAME)));

		for (int i = 0; i < servers.length * 3; i++) {
			client.send(serverMap.get((String) Util.getRandomOtherThan
					(serverMap.keySet(), crashed)), getCommand
					(updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME)));
		}
		Thread.sleep(SLEEP);

		verifyOrderConsistent(DEFAULT_TABLE_NAME, key, crashed,
				// can be empty (no liveness) if less than majority up
				serverMap.size() <= 2 * crashed.size());
	}

	/**
	 * Checks that the entire table matches across all servers. This test
	 * requires that recovering replicas can recover to a recent checkpoint of
	 * the state and replay missed requests from there onwards to catch up with
	 * what transpired when they were crashed.
	 * <p>
	 * There is always the default checkpoint of the empty initial state, but
	 * not checkpointing periodically means that the log of requests to replay
	 * while rolling forward upon recovery would keep growing unbounded.
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void test38_EntireStateMatchCheck() throws IOException,
			InterruptedException {
		verifyTableConsistent(DEFAULT_TABLE_NAME);
	}

	/**
	 * This test kills all servers simultaneously and then recovers them
	 * simultaneously. No requests are issued. Entire state should match at the
	 * end.
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */

	@Test
	public void test39_InstantaneousMassacreAndRevivalTest() throws
			IOException, InterruptedException {
		ServerFailureRecoveryManager.killAllServers();
		ServerFailureRecoveryManager.startAllServers();
		Thread.sleep(PER_SERVER_BOOTSTRAP_TIME * servers.length);
		test38_EntireStateMatchCheck();
	}

	/**
	 * Kills all servers serially and then recovers them serially, all while
	 * spraying requests at the servers.
	 * <p>
	 * Because request spraying is happening concurrently with violent massacre
	 * and benevolent rebirths, not all requests will be executed (liveness),
	 * but some should get executed, and the state should be identical at the
	 * end (safety). Furthermore, the key record is guaranteed to satisfy the
	 * non-emptiness check in
	 * {@link GraderFaultTolerance#verifyOrderConsistent}
	 * because of an earlier test that already populated the record with
	 * nonempty values.
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void test40_SerialKillAndRecover() throws IOException,
			InterruptedException {
		long interKillIntervalMillis = 500, interRecoverMillis = 800;
		// This method is asynchronous and will schedule and return
		// immediately.
		ServerFailureRecoveryManager.serialKillAndThenSerialRecover(servers
				.length, interKillIntervalMillis, interRecoverMillis);
		// no bootstrap time needed here

		// Spray requests while slaughter and rebirth is happening
		for (int i = 0; i < servers.length; i++) {
			client.send(serverMap.get(getRandomServerAddr()), getCommand
					(updateRecordOfTableCmd(fixedKeyKnownToExist,
							DEFAULT_TABLE_NAME)));
			Thread.sleep(Math.min(interKillIntervalMillis, interRecoverMillis)
					/ 3);
		}
		// sleep long enough for all servers to have recovered
		Thread.sleep((long) (PER_SERVER_BOOTSTRAP_TIME * servers.length +
				(interKillIntervalMillis + interRecoverMillis) * (1 +
						ServerFailureRecoveryManager.PERTURB_FRAC)));

		verifyOrderConsistent(DEFAULT_TABLE_NAME, fixedKeyKnownToExist);
	}


	/**
	 * This test tests checkpointing of state and restoration upon recovery
	 * by issuing more than MAX_LOG_SIZE requests.
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void test41_CheckpointRecoveryTest() throws IOException,
			InterruptedException {

		// issue enough requests to stress test checkpointing
		for (int i = 0; i < MAX_LOG_SIZE*5 + servers.length; i++) {
			client.send(serverMap.get(servers[0]), getCommand
					(updateRecordOfTableCmd(fixedKeyKnownToExist,
							DEFAULT_TABLE_NAME)));
			// small sleep to slow down because gigapaxos is optimized to
			// batch request flurries and may effectively see fewer
			// requests.
			Thread.sleep(10);
		}
		ServerFailureRecoveryManager.mercilesslySlaughterAll();
		ServerFailureRecoveryManager.startAllServers();
		Thread.sleep(PER_SERVER_BOOTSTRAP_TIME * servers.length);

		// specific key
		verifyOrderConsistent(DEFAULT_TABLE_NAME, fixedKeyKnownToExist);
		// entire state
		test38_EntireStateMatchCheck();
	}


	/**
	 * Clean up the default tables. Will always succeed irrespective of student
	 * code. Identical to test19_DropTables in GraderConsistency.
	 * <p>
	 * For debugging purposes, if you wish to manually inspect the tables to
	 * see
	 * what's not matching, you can temporarily disable this test in your local
	 * copy. New {@link GraderFaultTolerance} tests will always clear the table
	 * before the first test, so you don't have to worry about cleaning
	 * leftover
	 * state from previous runs.
	 *
	 * @throws InterruptedException
	 */
	@Test
	public void test49_DropTables() throws InterruptedException {
		Thread.sleep(MAX_SLEEP);
		for (String node : servers) {
			// clean up
			session.execute(getDropTableCmd(DEFAULT_TABLE_NAME, node));
		}
	}

	/**
	 * This test should always pass. Unless unnecessary timers or threads are
	 * introduced by students.
	 */
	public void test99_closeSessionAndServers() {

		session.close();
		// If gigapaxos and single JVM servers, we need to close created
		// classes. If separate processes, the shutdown hook automatically
		// kills them.
		if (GIGAPAXOS_MODE && DEFER_SERVER_CREATION_TO_CHILD)
			for (ReconfigurableNode node : gpServers)
				node.close();
	}

	private static InetSocketAddress getRandomServerAddr() {
		return serverMap.get(ServerFailureRecoveryManager.getRandomServer());
	}

	// verify that the key actually exists on server
	private boolean verifyInserted(int key, String server) {
		ResultSet result = session.execute(readResultFromTableCmd(key,
				DEFAULT_TABLE_NAME, server));
		return result.isExhausted();
	}

	protected void verifyTableConsistent(String table) {
		verifyTableConsistent(table, false);
	}

	protected void verifyTableConsistent(String table, boolean possiblyEmpty) {
		verifyOrderConsistent(table, (Integer) null, new HashSet<String>(),
				possiblyEmpty);
	}


	protected void verifyOrderConsistent(String table, int key, Set<String>
			exclude) {
		verifyOrderConsistent(table, key, exclude, false);
	}

	public static void main(String[] args) throws IOException {
		addShutdownHook();
		Result result = JUnitCore.runClasses(GraderFaultTolerance.class);
	}
}
