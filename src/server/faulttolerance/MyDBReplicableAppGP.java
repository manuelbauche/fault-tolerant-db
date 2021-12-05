package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 * Faster
	 * is not necessarily better, so don't sweat speed. Focus on safety.
	 */
	public static final int SLEEP = 1000;
	final private Session session;
	final private Cluster cluster;
	private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
	final String KEYSPACE;
	public static final String
			CASSANDRA_HOME_PATH="C:/Users/manue/Cassandra/apache-cassandra-4.0.1";

	/**
	 * Set this string as need to specify the path for cassandra's cqlsh
	 * command, i.e., you can invoke cqlsh as CASSANDRA_HOME_PATH/bin/cqlsh
	 */
	public static final String
			CASSANDRA_HOME_PATH="/Users/arun/apache-cassandra-3.11.11";

	/**
	 * All Gigapaxos apps must either support a no-args constructor or a
	 * constructor taking a String[] as the only argument. Gigapaxos relies on
	 * adherence to this policy in order to be able to reflectively construct
	 * customer application instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect.
	 *             Optional args[1] and args[2]
	 * @throws IOException
	 */
	public MyDBReplicableAppGP(String[] args) throws IOException {
		// TODO: setup connection to the data store and keyspace
		if (args.length == 0) {
			session = (cluster=Cluster.builder().addContactPoint("127.0.0.1").build()).connect("demo");
		}
		else {
			session = (cluster=Cluster.builder().addContactPoint("127.0.0.1").build()).connect(args[0].toString());
		}
		KEYSPACE = args[0];
	}

	/**
	 * Refer documentation of {@link Replicable#execute(Request, boolean)} to
	 * understand what the boolean flag means.
	 * <p>
	 * You can assume that all requests will be of type {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
	 *
	 * @param request
	 * @param b
	 * @return
	 */
	@Override
	public boolean execute(Request request, boolean b) {
		// TODO: submit request to data store
		try {
			RequestPacket sendRequest = (RequestPacket)request; // Casts request into request packet (just to be sure)
			ResultSet results = session.execute(sendRequest.getRequestValue()); // Sends request to open session

			// Reply to client
			if (!b) {
				String response = "";
				for (int i = 0; i < results.all().size(); i++) {
					response += results.all().get(i).toString();
				}
				sendRequest.setResponse(response);
			}
			return true;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;

	}

	/**
	 * Refer documentation of
	 * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
	 *
	 * @param request
	 * @return
	 */
	@Override
	public boolean execute(Request request) {
		// TODO: execute the request by sending it to the data store
		return execute(request, true);
	}

	private static final String FORMAT = "state-%s_%s.csv";
	private static final String TABLE = "grade";
	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
	public String checkpoint(String s) {
		// TODO:
		String response = String.format(FORMAT, KEYSPACE, TIMESTAMP_FORMAT.format(new Date()));

		Runtime runtime = Runtime.getRuntime();
		try {
			runtime.exec(String.format(CASSANDRA_HOME_PATH + "/bin/cqlsh ", "-e ", "COPY %s TO %s", TABLE, response));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return response;
	}

	/**
	 * Refer documentation of {@link Replicable#restore(String, String)}
	 *
	 * @param s
	 * @param s1
	 * @return
	 */
	@Override
	public boolean restore(String s, String s1) {
		// TODO:
		try {
			Runtime.getRuntime().exec(String.format(CASSANDRA_HOME_PATH + "/bin/cqlsh -e COPY %s FROM %s", TABLE, s1));
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}


	/**
	 * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
	 * .RequestPacket will be used by Grader, so you don't need to implement
	 * this method.}
	 *
	 * @param s
	 * @return
	 * @throws RequestParseException
	 */
	@Override
	public Request getRequest(String s) throws RequestParseException {
		return null;
	}

	/**
	 * @return Return all integer packet types used by this application. For an
	 * example of how to define your own IntegerPacketType enum, refer {@link
	 * edu.umass.cs.reconfiguration.examples.AppRequest}. This method does not
	 * need to be implemented because the assignment Grader will only use
	 * {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}
}