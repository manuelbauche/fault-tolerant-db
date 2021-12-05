import client.Client;
import client.MyDBClient;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.RepeatRule;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import server.*;
import server.faulttolerance.MyDBFaultTolerantServerZK;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class sets up the testing environment needed by GraderConsistency.
 */
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class GraderCommonSetup {

	/* For SingleServer, this is the default keyspace. For replicated
	servers, each server uses a keyspace named after the server's name itself.
	All keyspaces are being created in the testing setup() method below.
	 */
	protected static final String DEFAULT_KEYSPACE = "demo";
	protected static final InetSocketAddress DEFAULT_SADDR = new
			InetSocketAddress("localhost", 1999);
	private static final InetSocketAddress DEFAULT_DB_ADDR = new
			InetSocketAddress("localhost", 9042);

	// sleep macros
	protected static final int MAX_SLEEP = 1000;
	protected static int SLEEP = Math.min(MAX_SLEEP, MyDBFaultTolerantServerZK
			.SLEEP);
	public static final int PER_SERVER_BOOTSTRAP_TIME = MAX_SLEEP * 4;


	/* True means servers will be started as separate OS-level processes
	as opposed to objects all instantiated within a single JVM.
	 */
	protected static final boolean PROCESS_MODE = true;


	/* Set to true if you want each server's output and error in a separate
	file named <node>.out where <node> is the server's name from the
	conf.properties file.
	 */
	protected static final boolean REDIRECT_IO_TO_FILE = false;


	// True only for testing use by instructor. It can be used to start
	// gigapaxos servers in a single JVM if set to true and PROCESS_MODE is
	// set to false.
	protected static boolean DEFER_SERVER_CREATION_TO_CHILD = false;

	// If true, replicas will start with whatever DB table state they had
	// just before they last crashed. Should be false while submitting.
	// But you can set it to true for debugging/testing tests that don't
	// involve checkpointing.
	protected static boolean DISABLE_RECOVERING_WITH_EMPTY_STATE = false;


	/* May need to prefix path with "consistency" folder if starting
	from 590cc folder.

	 Protected because ServerFailureRecoveryManager also accesses this
	 information to provide it as a command-line arg to start servers as
	 separate processes.
	 */
	protected static final String CONFIG_FILE = System.getProperty("config")
			!= null ? System.getProperty("config") : (!GraderFaultTolerance
			.GIGAPAXOS_MODE ? "conf/servers" + ".properties" : "conf/gigapaxos" +
			".properties");

	// Must be true when testing fault tolerance; false means just replicated
	// consistency will be tested.
	protected static boolean TEST_FAULT_TOLERANCE = true;

	// Must be true when used by students
	protected static final boolean STUDENT_TESTING_MODE = true;

	// This is the table storing all server-side state. By assumption, the
	// state in this table in the keyspace corresponding to each replica is
	// all of the safety-critical state to be managed consistently.
	protected final static String DEFAULT_TABLE_NAME = "grade";


	protected static Client client = null;
	private static SingleServer singleServer = null;
	private static SingleServer[] replicatedServers = null;
	protected static Map<String, InetSocketAddress> serverMap = null;
	protected static String[] servers = null;
	private static Cluster cluster;
	protected static Session session = (cluster = Cluster.builder()
			.addContactPoint(DEFAULT_SADDR.getHostName()).build()).connect
			(DEFAULT_KEYSPACE);

	protected static NodeConfig<String> nodeConfigServer;

	protected static final int NUM_REQS = 100;

	protected static final int SLEEP_RATIO = 10;

	@BeforeClass
	public static void setup() throws IOException, InterruptedException {
		// setup single server
		singleServer = STUDENT_TESTING_MODE ? new MyDBSingleServer
				(DEFAULT_SADDR, DEFAULT_DB_ADDR, DEFAULT_KEYSPACE) :
				(SingleServer) getInstance(getConstructor("server" + "" + "" +
						".AVDBSingleServer", InetSocketAddress.class,
						InetSocketAddress.class, String.class), DEFAULT_SADDR,
						DEFAULT_DB_ADDR, DEFAULT_KEYSPACE);
		nodeConfigServer = NodeConfigUtils.getNodeConfigFromFile(CONFIG_FILE,
				(!GraderFaultTolerance.GIGAPAXOS_MODE ? ReplicatedServer
						.SERVER_PREFIX : PaxosConfig.DEFAULT_SERVER_PREFIX),
				(!GraderFaultTolerance.GIGAPAXOS_MODE ? ReplicatedServer
						.SERVER_PORT_OFFSET: 0));
		//ServerFailureRecoveryManager.setNodeConfigServer(nodeConfigServer);

		/* Setup client here. Will instantiate MyDBClient here because
		STUDENT_MODE is true by default.
		 */
		NodeConfig<String> nodeConfigClient = NodeConfigUtils
				.getNodeConfigFromFile(CONFIG_FILE, (!GraderFaultTolerance.GIGAPAXOS_MODE ? ReplicatedServer
						.SERVER_PREFIX : PaxosConfig.DEFAULT_SERVER_PREFIX),
						(!GraderFaultTolerance.GIGAPAXOS_MODE ? 0:
								PaxosConfig.getClientPortOffset()));
		client = STUDENT_TESTING_MODE ? new MyDBClient(nodeConfigClient)

				// instructor client
				: (Client) getInstance(getConstructor("client.AVDBClient",
				NodeConfig.class), nodeConfigClient);


		// setup replicated servers and sessions to test
		replicatedServers = new SingleServer[nodeConfigServer.getNodeIDs()
				.size()];

		// create keyspaces if not exists
		session.execute("create keyspace if not exists " + DEFAULT_KEYSPACE +
				" with " + "replication={'class':'SimpleStrategy', " +
				"'replication_factor' : '1'};");
		for (String node : nodeConfigServer.getNodeIDs()) {
			session.execute("create keyspace if not exists " + node + " with "
					+ "replication={'class':'SimpleStrategy', " +
					"'replication_factor' : '1'};");
		}

		// setup frequently used information
		int i = 0;
		servers = new String[nodeConfigServer.getNodeIDs().size()];
		for (String node : nodeConfigServer.getNodeIDs())
			servers[i++] = node;
		serverMap = new HashMap<String, InetSocketAddress>();
		for (String node : nodeConfigClient.getNodeIDs())
			serverMap.put(node, new InetSocketAddress(nodeConfigClient
					.getNodeAddress(node), nodeConfigClient.getNodePort
					(node)));

		// Servers may be set up either as instantiated objects all within a
		// single JVM or as separate processes.
		startReplicatedServers();
	}

	//////////////////////// Test watching setup ///////////////////
	@Rule
	public TestName testName = new TestName();
	@Rule
	public RepeatRule repeatRule = new RepeatRule();
	@Rule
	public TestWatcher watcher = new TestWatcher() {
		protected void failed(Throwable e, Description description) {
			System.out.println(" FAILED!!!!!!!!!!!!! " + e);
			e.printStackTrace();
			System.exit(1);
		}

		protected void succeeded(Description description) {
			System.out.println(" succeeded");
		}
	};

	@Before
	public void beforeMethod() {
		System.out.print(this.testName.getMethodName() + " ");
	}
	/////////////////////////////////////////////////////////


	protected static void createEmptyTables() throws InterruptedException {
		for (String node : servers) {
			// create default table, node is the keypsace name
			session.execute(getCreateTableWithList(DEFAULT_TABLE_NAME, node));
			session.execute(getClearTableCmd(DEFAULT_TABLE_NAME, node));
		}
	}


	/**
	 * Clean up the default tables. Will always succeed irrespective of student
	 * code.
	 *
	 * @throws InterruptedException
	 */
	@Test
	public void test19_DropTables() throws InterruptedException {
		for (String node : servers) {
			// clean up
			session.execute(getDropTableCmd(DEFAULT_TABLE_NAME, node));
		}
	}


	// Will always succeed irrespective of student code.
	@Test
	public void test99_closeSession() {
		session.close();
	}

	protected void testCreateTable(boolean single, boolean sleep) throws
			IOException, InterruptedException {
		if (sleep) testCreateTableSleep(single);
		else testCreateTableBlocking();
		verifyTableExists(TABLE, DEFAULT_KEYSPACE, true);

	}

	protected void verifyTableExists(String table, String keyspace, boolean
			exists) {
		ResultSet resultSet = session.execute("select table_name from " +
				"system_schema.tables where keyspace_name='" + keyspace + "'");
		Assert.assertTrue(!resultSet.isExhausted());
		boolean match = false;
		for (Row row : resultSet)
			match = match || row.getString("table_name").equals(table);
		if (exists) Assert.assertTrue(match);
		else Assert.assertFalse(match);
	}

	protected void verifyOrderConsistent(String table, int key) {
		verifyOrderConsistent(table, key, new HashSet<String>(), false);
	}

	protected void verifyOrderConsistent(String table, Integer key,
										 Set<String> exclude, boolean
												 possiblyEmpty) {
		String[] results = new String[servers.length - exclude.size()];
		String[] comparedServers = new String[servers.length - exclude.size()];

		int j = 0;
		for (int i = 0; i < servers.length; i++)
			if (!exclude.contains(servers[i]))
				comparedServers[j++] = servers[i];

		int i = 0;
		boolean nonEmpty = false;
		for (String node : comparedServers) {
			ResultSet result = session.execute(key != null ?
					readResultFromTableCmd(key, table, node) :
					readResultFromTableCmd(table, node));

			results[i] = "";
			for (Row row : result) {
				results[i] += row;
				nonEmpty = true;
			}
			i++;
		}

		boolean match = true;
		String message = "";
		for (i = 0; i < results.length; i++) {
			if (!results[0].equals(results[i])) {
				match = false;
				message += "\n" + comparedServers[0] + ":" + results[0] + "\n " +
						"" + "" + "" + "" + "" + "" + "" + "" + "" + "" + "" +
						"" + "" + "" + "" + "" + "" + "" + "" + "  " + "!=\n"
						+ comparedServers[i] + ":" + results[i] + "\n";
			}
			i++;
		}
		message += (!(possiblyEmpty || nonEmpty) ? "nonEmpty=" + nonEmpty :
				"");
		if (!exclude.isEmpty())
			System.out.println("Excluded servers=" + exclude);
		System.out.println("\n");
		for (i = 0; i < results.length; i++)
			System.out.println(comparedServers[i] + ":" + results[i]);
		Assert.assertTrue(message, (possiblyEmpty || nonEmpty) && match);
	}


	private void testCreateTableSleep(boolean single) throws
			InterruptedException, IOException {
		send(getDropTableCmd(TABLE, DEFAULT_KEYSPACE), single);
		Thread.sleep(SLEEP);
		send(getCreateTableCmd(TABLE, DEFAULT_KEYSPACE), single);
		Thread.sleep(SLEEP);
	}


	ConcurrentHashMap<Long, String> outstanding = new ConcurrentHashMap<Long,
			String>();

	private void testCreateTableBlocking() throws InterruptedException,
			IOException {
		waitResponse(callbackSend(DEFAULT_SADDR, getDropTableCmd(TABLE,
				DEFAULT_KEYSPACE)));
		waitResponse(callbackSend(DEFAULT_SADDR, getCreateTableCmd(TABLE,
				DEFAULT_KEYSPACE)));
	}


	protected Long callbackSend(InetSocketAddress isa, String request) throws
			IOException {
		Long id = enqueueRequest(request);
		client.callbackSend(isa, request, new WaitCallback(id));
		return id;
	}

	protected Long callbackSend(InetSocketAddress isa, String request, long
			timeout) throws IOException {
		Long id = enqueueRequest(request);
		client.callbackSend(isa, request, new WaitCallback(id));
		return id;
	}

	private class WaitCallback implements Client.Callback {
		Long monitor; // both id and monitor

		WaitCallback(Long monitor) {
			this.monitor = monitor;
		}

		@Override
		public void handleResponse(byte[] bytes, NIOHeader header) {
			synchronized (this.monitor) {
				outstanding.remove(monitor);
				this.monitor.notify();
			}
		}
	}

	private long reqnum = 0;

	private long enqueue() {
		synchronized (outstanding) {
			return reqnum++;
		}
	}

	private long enqueueRequest(String request) {
		long id = enqueue();
		outstanding.put(id, request);
		return id;
	}

	protected void waitResponse(Long id) {
		synchronized (id) {
			while (outstanding.containsKey(id)) try {
				id.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	protected void waitResponse(Long id, long timeout) {
		synchronized (id) {
			while (outstanding.containsKey(id)) try {
				id.wait(timeout);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}


	protected static final void send(String cmd, boolean single) throws
			IOException {
		client.send(single ? DEFAULT_SADDR : serverMap.get(servers[(int) (Math
				.random() * servers.length)]), cmd);
	}

	protected static final String TABLE = "users";

	private static String getCreateTableCmd(String table, String keyspace) {
		return "create table if not exists " + keyspace + "." + table + " " +
				"(age" + " int, firstname " + "text, lastname text, ssn int, "
				+ "address " + "text, hash bigint, " + "primary key (ssn))";
	}

	protected static String getDropTableCmd(String table, String keyspace) {
		return "drop table if exists " + keyspace + "." + table;
	}

	protected static String getClearTableCmd(String table, String keyspace) {
		return "truncate " + keyspace + "." + table;
	}

	protected static String getCreateTableWithList(String table, String
			keyspace) {
		return "create table if not exists " + keyspace + "." + table + " (id " +
				"" + "" + "" + "" + "" + "" + "" + "" + "" + "int," + " " +
				"events" + "" + "" + " " + "list<int>, " + "primary " + "" +
				"key" + " " + "" + "" + "" + "(id)" + ");";
	}

	protected static String insertRecordIntoTableCmd(int key, String table) {
		return "insert into " + table + " (id, events) values (" + key + ", "
				+ "[]);";
	}

	protected static String updateRecordOfTableCmd(int key, String table) {
		return "update " + table + " SET events=events+[" + incrSeq() + "] " +
				"where id=" + key + ";";
	}

	// reads the entire table, all keys
	protected static String readResultFromTableCmd(String table, String
			keyspace) {
		return "select events from " + keyspace + "." + table + ";";
	}

	// This is only used to fetch the result from the table by session
	// directly connected to cassandra
	protected static String readResultFromTableCmd(int key, String table,
												   String keyspace) {
		return "select events from " + keyspace + "." + table + " where id=" +
				key + ";";
	}

	private static long sequencer = 0;

	synchronized static long incrSeq() {
		return sequencer++;
	}

	private static void startReplicatedServersSingleJVM() throws IOException {
		int i = 0;
		for (String node : nodeConfigServer.getNodeIDs()) {
			replicatedServers[i++] = STUDENT_TESTING_MODE ?
					(TEST_FAULT_TOLERANCE ? new MyDBFaultTolerantServerZK
							(nodeConfigServer, node, DEFAULT_DB_ADDR) : new
							MyDBReplicatedServer(nodeConfigServer, node,
							DEFAULT_DB_ADDR)) :

					// instructor mode
					(SingleServer) getInstance(getConstructor
							("server.AVDBReplicatedServer", NodeConfig
									.class, String.class, InetSocketAddress
									.class), nodeConfigServer, node,
							DEFAULT_DB_ADDR);
		}
	}


	private static void startReplicatedServers() throws IOException,
			InterruptedException {
		if (TEST_FAULT_TOLERANCE && !DISABLE_RECOVERING_WITH_EMPTY_STATE)
			createEmptyTables();

		// creates instances of replicated servers in single JVM. This option
		// should not be used for near-final-testing fault tolerance because we
		// don't have a way to "crash" servers except in PROCESS_MODE. But
		// PROCESS_MODE can be disabled if you wish to debug graceful mode
		// execution.
		if (!PROCESS_MODE) startReplicatedServersSingleJVM();
			// creates instances of replicated servers in separate processes
			// provided if it is not meant to be deferred to child
			// implementations of this testing class.
		else if (!(GraderFaultTolerance.GIGAPAXOS_MODE && DEFER_SERVER_CREATION_TO_CHILD))
			ServerFailureRecoveryManager.startAllServers();
	}

	@AfterClass
	public static void teardown() {
		if (client != null) client.close();
		if (singleServer != null) singleServer.close();
		session.close();
		cluster.close();

		if (PROCESS_MODE) ServerFailureRecoveryManager.killAllServers();
		else if (replicatedServers != null)
			for (SingleServer s : replicatedServers)
				if (s != null) s.close();
	}

	protected static Object getInstance(Constructor<?> constructor, Object...
			args) {
		try {
			return constructor.newInstance(args);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected static Constructor<?> getConstructor(String clazz, Class<?>...
			types) {
		try {
			Class<?> instance = Class.forName(clazz);
			return instance.getConstructor(types);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected static void addShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				if (PROCESS_MODE) ServerFailureRecoveryManager
						.killAllServers();
				;
			}
		});
	}

	public static void main(String[] args) throws IOException {
		addShutdownHook();
		Result result = JUnitCore.runClasses(GraderCommonSetup.class);
	}
}
