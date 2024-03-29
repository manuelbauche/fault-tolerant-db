package server;

import client.MyDBClient;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

/**
 * Consistency
 *
 * This class should implement your replicated database server. Refer to
 * {@link ReplicatedServer} for a starting point.
 */
public class MyDBReplicatedServer extends MyDBSingleServer {
    final private Session session;
    final private Cluster cluster;

    protected final String myID;
    protected final MessageNIOTransport<String,String> serverMessenger;

    protected String leader;

    // this is the message queue used to track which messages have not been sent yet
    private ConcurrentHashMap<Long, JSONObject> queue = new ConcurrentHashMap<Long, JSONObject>();
    private CopyOnWriteArrayList<String> notAcked;

    // the sequencer to track the most recent request in the queue
    private static long reqnum = 0;
    synchronized static Long incrReqNum() {
        return reqnum++;
    }

    // the sequencer to track the next request to be sent
    private static long expected = 0;
    synchronized static Long incrExpected() {
        return expected++;
    }

    /**
     * @param nodeConfig
     * @param myID
     * @param isaDB
     * @throws IOException
     */
    public MyDBReplicatedServer(NodeConfig<String> nodeConfig, String myID,
                                InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID)-ReplicatedServer
                        .SERVER_PORT_OFFSET), isaDB, myID);
        session = (cluster=Cluster.builder().addContactPoint("127.0.0.1")
                .build()).connect(myID);
        log.log(Level.INFO, "Server {0} added cluster contact point",	new
                Object[]{myID,});
        // leader is elected as the first node in the nodeConfig
        for(String node : nodeConfig.getNodeIDs()){
            this.leader = node;
            break;
        }

        this.myID = myID;

        this.serverMessenger =  new
                MessageNIOTransport<String, String>(myID, nodeConfig,
                new
                        AbstractBytePacketDemultiplexer() {
                            @Override
                            public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                                handleMessageFromServer(bytes, nioHeader);
                                return true;
                            }
                        }, true);

        log.log(Level.INFO, "Server {0} started on {1}", new Object[]{this
                .myID, this.clientMessenger.getListeningSocketAddress()});

    }

    protected static enum Type {
        REQUEST, // a server forwards a REQUEST to the leader
        PROPOSAL, // the leader broadcast the REQUEST to all the nodes
        ACKNOWLEDGEMENT; // all the nodes send back acknowledgement to the leader
    }
    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {

        // this is a request sent by callbackSend method
        String request = new String(bytes);

        log.log(Level.INFO, "{0} received client message {1} from {2}",
                new Object[]{this.myID, request, header.sndr});
        JSONObject json = null;
        try {
            json = new JSONObject(request);
            request = json.getString(MyDBClient.Keys.REQUEST
                    .toString());
        } catch (JSONException e) {
            //e.printStackTrace();
        }

        // forward the request to the leader as a proposal
        try {
            JSONObject packet = new JSONObject();
            packet.put(MyDBClient.Keys.REQUEST.toString(), request);
            packet.put(MyDBClient.Keys.TYPE.toString(), MyDBReplicatedServer.Type.REQUEST.toString());

            this.serverMessenger.send(leader, packet.toString().getBytes());
            log.log(Level.INFO, "{0} sends a REQUEST {1} to {2}",
                    new Object[]{this.myID, packet, leader});
        } catch (IOException | JSONException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }


        String response = "[success:"+new String(bytes)+"]";
        if(json!=null){
            try{
                json.put(MyDBClient.Keys.RESPONSE.toString(),
                        response);
                response = json.toString();
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        try{
            // when it's done send back response to client
            serverMessenger.send(header.sndr, response.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {

        // deserialize the request
        JSONObject json = null;
        try {
            json = new JSONObject(new String(bytes));
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        log.log(Level.INFO, "{0} received relayed message {1} from {2}",
                new Object[]{this.myID, json, header.sndr}); // simply log

        // check the type of the request
        try {
            String type = json.getString(MyDBClient.Keys.TYPE.toString());
            if (type.equals(MyDBReplicatedServer.Type.REQUEST.toString())){
                if(myID.equals(leader)){

                    // put the request into the queue
                    Long reqId = incrReqNum();
                    json.put(MyDBClient.Keys.REQNUM.toString(), reqId);
                    queue.put(reqId, json);
                    log.log(Level.INFO, "{0} put request {1} into the queue.",
                            new Object[]{this.myID, json});

                    if(isReadyToSend(expected)){
                        // retrieve the first request in the queue
                        JSONObject proposal = queue.remove(expected);
                        if(proposal != null) {
                            proposal.put(MyDBClient.Keys.TYPE.toString(), MyDBReplicatedServer.Type.PROPOSAL.toString());
                            enqueue();
                            broadcastRequest(proposal);
                        } else {
                            log.log(Level.INFO, "{0} is ready to send request {1}, but the message has already been retrieved.",
                                    new Object[]{this.myID, expected});
                        }

                    }
                } else {
                    log.log(Level.SEVERE, "{0} received REQUEST message from {1} which should not be here.",
                            new Object[]{this.myID, header.sndr});
                }
            } else if (type.equals(MyDBReplicatedServer.Type.PROPOSAL.toString())) {

                // execute the query and send back the acknowledgement
                String query = json.getString(MyDBClient.Keys.REQUEST.toString());
                long reqId = json.getLong(MyDBClient.Keys.REQNUM.toString());

                session.execute(query);

                JSONObject response = new JSONObject().put(MyDBClient.Keys.RESPONSE.toString(), this.myID)
                        .put(MyDBClient.Keys.REQNUM.toString(), reqId)
                        .put(MyDBClient.Keys.TYPE.toString(), MyDBReplicatedServer.Type.ACKNOWLEDGEMENT.toString());
                serverMessenger.send(header.sndr, response.toString().getBytes());
            } else if (type.equals(MyDBReplicatedServer.Type.ACKNOWLEDGEMENT.toString())) {

                // only the leader needs to handle acknowledgement
                if(myID.equals(leader)){
                    // TODO: leader processes ack here
                    String node = json.getString(MyDBClient.Keys.RESPONSE.toString());
                    if (dequeue(node)){
                        // if the leader has received all acks, then prepare to send the next request
                        expected++;
                        if(isReadyToSend(expected)){
                            JSONObject proposal = queue.remove(expected);
                            if(proposal != null) {
                                proposal.put(MyDBClient.Keys.TYPE.toString(), MyDBReplicatedServer.Type.PROPOSAL.toString());
                                enqueue();
                                broadcastRequest(proposal);
                            } else {
                                log.log(Level.INFO, "{0} is ready to send request {1}, but the message has already been retrieved.",
                                        new Object[]{this.myID, expected});
                            }
                        }
                    }
                } else {
                    log.log(Level.SEVERE, "{0} received ACKNOWLEDEMENT message from {1} which should not be here.",
                            new Object[]{this.myID, header.sndr});
                }
            } else {
                log.log(Level.SEVERE, "{0} received unrecongonized message from {1} which should not be here.",
                        new Object[]{this.myID, header.sndr});
            }

        } catch (JSONException | IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }


    private boolean isReadyToSend(long expectedId) {
        if (queue.size() > 0 && queue.containsKey(expectedId)) {
            return true;
        }
        return false;
    }

    private void broadcastRequest(JSONObject req) {
        for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()){
            try {
                this.serverMessenger.send(node, req.toString().getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        log.log(Level.INFO, "The leader has broadcast the request {0}", new Object[]{req});
    }

    private void enqueue(){
        notAcked = new CopyOnWriteArrayList<String>();
        for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()){
            notAcked.add(node);
        }
    }

    private boolean dequeue(String node) {
        if(!notAcked.remove(node)){
            log.log(Level.SEVERE, "The leader does not have the key {0} in its notAcked", new Object[]{node});
        }
        if(notAcked.size() == 0)
            return true;
        return false;
    }

    @Override
    public void close() {
        super.close();
        this.serverMessenger.stop();
        session.close();
        cluster.close();
    }

    /**
     *
     * @param args args[0] must be server.properties file and args[1] must be
     *               myID. The server prefix in the properties file must be
     *               ReplicatedServer.SERVER_PREFIX.
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        new MyDBReplicatedServer(NodeConfigUtils.getNodeConfigFromFile
                (args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer
                        .SERVER_PORT_OFFSET), args[1], new InetSocketAddress
                ("localhost", 9042));
    }
}