package helios.datacenter;

import helios.log.Log;
import helios.message.ClientMessageType;
import helios.message.ClientRequestMessage;
import helios.message.LogPropMessage;
import helios.message.MessageWrapper;
import helios.misc.Common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * @Project: helios
 * @Title: DataCenter.java
 * @Package helios
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 27, 2016 10:46:04 PM
 * @version V1.0
 */
public class DataCenter implements Runnable {
    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;

    // UNIQUE routing key
    private String dataCenterName;

    // Index in dataCenterList
    private int dataCenterIndex;

    // Log queue
    // private String fanoutQueueName;
    // Client message queue
    private String clientMessageDirectQueueName;

    private String logPropagationDirectQueueName;

    // Use location to calculated the simulated RTT between datacenters;
    private int location;
    // Consumer for client message and log message
    private QueueingConsumer consumer;

    // Need to know all data center names for routing
    private String[] dataCenterNames;
    // Need to know all data center locations for generating fake RTT
    private int[] dataCenterLocations;
    private int totalNumOfDataCenters;
    // Now I just understand one row's function... Just use it when periodically
    private long[] rDict;
    // To simulate log propagation latencies
    private HashMap<String, Integer> RTTLatencies;
    // Each center is assigned a commitOffset to other data center, used to decide whether to commit or abort
    private HashMap<String, Integer> commitOffsets;

    private PriorityQueue<Log> PTPool;
    private PriorityQueue<Log> EPTPool;

    private boolean isAutoLogPropagation = true;

    private static Logger logger = Logger.getLogger(DataCenter.class);

    /**
     * Initialization
     * 
     * @param dataCenterName
     * @param totalNumOfDataCenters
     * @throws Exception
     */
    public DataCenter(String dataCenterName, int dataCenterIndex, int location, String[] dataCenterNames,
            int[] dataCenterLocations)
            throws Exception {
        super();
        this.dataCenterName = dataCenterName;
        this.dataCenterIndex = dataCenterIndex;
        this.location = location;
        this.dataCenterNames = dataCenterNames;
        this.dataCenterLocations = dataCenterLocations;

        this.totalNumOfDataCenters = dataCenterNames.length;
        this.RTTLatencies = new HashMap<String, Integer>();
        this.commitOffsets = new HashMap<String, Integer>();

        clientMessageDirectQueueName = Common.getClientMessageDirectQueueName(dataCenterName);
        logPropagationDirectQueueName = Common.getLogPropgationDirectQueueName(dataCenterName);

        factory = new ConnectionFactory();
        factory.setHost(Common.MQ_HOST_NAME);
        connection = factory.newConnection();
        channel = connection.createChannel();
        rDict = new long[totalNumOfDataCenters];
        PTPool = new PriorityQueue<Log>();
        EPTPool = new PriorityQueue<Log>();
    }

    // Generate RTT list based on location setting of data centers
    public void generateRTTList() {
        for (int i = 0; i < dataCenterLocations.length; i++) {
            // this.RTTLatencies[i] = Math.abs(this.location - dataCenterLocations[i]);
            this.RTTLatencies.put(dataCenterNames[i], Math.abs(this.location - dataCenterLocations[i]));
        }
    }

    // Temporarily use 0 as all commit offsets
    public void generateCommitOffsets() {
        for (int i = 0; i < dataCenterNames.length; i++) {
            this.commitOffsets.put(dataCenterNames[i], 0);
        }
    }

    /**
     * Bind datacenter to a direct exchange to receive log propgation message.
     * Queue name: this.dataCenterName + "log.direct.queue";, Routing Key: this.dataCenterName
     * Description: TODO
     * void
     * 
     * @throws Exception
     */
    public void bindToLogExchange() throws Exception {
        channel.exchangeDeclare(Common.LOG_DIRECT_EXCHANGE_NAME, "direct");
        channel.queueDeclare(this.logPropagationDirectQueueName, false, false, false, null);
        // 使用logPropagationDirectQueueName这个Queue绑定到Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME这个exchange上，routing
        // key为dataCenterName
        channel.queueBind(this.logPropagationDirectQueueName, Common.LOG_DIRECT_EXCHANGE_NAME,
                this.dataCenterName);
        if (consumer == null) {
            consumer = new QueueingConsumer(channel);
        }
        channel.basicConsume(this.logPropagationDirectQueueName, true, consumer);
    }

    /**
     * Bind datacenter to a direct exchange to receive client message.
     * Queue name: this.dataCenterName + ".direct.queue", Routing Key: this.dataCenterName
     * Description: TODO
     * void
     * 
     * @throws Exception
     */
    public void bindToClientExchange() throws Exception {
        channel.exchangeDeclare(Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME, "direct");
        channel.queueDeclare(this.clientMessageDirectQueueName, false, false, false, null);

        // 使用clientMessageDirectQueueName这个Queue绑定到Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME这个exchange上，routing
        // key为dataCenterName
        channel.queueBind(this.clientMessageDirectQueueName, Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME,
                this.dataCenterName);
        if (consumer == null) {
            consumer = new QueueingConsumer(channel);
        }
        channel.basicConsume(clientMessageDirectQueueName, true, consumer);

    }

    /**
     * 
     * Description: Send log propagation message
     * 
     * @param message
     * @throws IOException
     *             void
     */
    public void sendLogPropagationMessage(String routingKey, LogPropMessage message) throws IOException {
        channel.basicPublish(Common.LOG_DIRECT_EXCHANGE_NAME, routingKey, null,
                new MessageWrapper(Common.Serialize(message), message.getClass()).getSerializedMessage().getBytes());
    }

    private long getCurrentTimestamp() {
        return System.currentTimeMillis() / 1000L;
    }

    @Override
    protected void finalize() throws Throwable {
        // TODO Auto-generated method stub
        super.finalize();
        this.channel.close();
        this.connection.close();
    }

    /**
     * Datacenter run method, wait for incoming client request
     */
    public void run() {
        MessageWrapper wrapper = null;
        QueueingConsumer.Delivery delivery;
        logger.info("Data Center: " + this.dataCenterName + " is Running");
        try {
            this.bindToClientExchange();
        } catch (Exception e) {
            logger.error(this.dataCenterName + " binding to client exchange failed");
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            this.bindToLogExchange();
        } catch (Exception e1) {
            logger.error(this.dataCenterName + " binding to log exchange failed");
            e1.printStackTrace();
            System.exit(-1);
        }
        // Start Log propagation
        for (int i = 0; i < dataCenterNames.length; i++) {
            if (!dataCenterNames[i].equals(this.dataCenterName))
                // Propgate log to datacenter other than itself
                new Thread(new LogPropagationThread(this, dataCenterNames[i])).start();
        }
        // Receive Log
        try {
            while (true) {
                delivery = consumer.nextDelivery();
                if (delivery != null) {
                    String msg = new String(delivery.getBody());
                    wrapper = MessageWrapper.getDeSerializedMessage(msg);
                }
                if (wrapper != null) {
                    if (wrapper.getmessageclass() == ClientRequestMessage.class) {
                        ClientRequestMessage request = (ClientRequestMessage) wrapper.getDeSerializedInnerMessage();
                        // System.out.println("BEGIN REQUEST");
                        logger.info("Client message received");
                        ClientMessageType t = request.getType();
                        switch (t) {
                        case BEGIN:
                            logger.info("BEGIN MESSAGE");
                            break;
                        case WRITE:
                            logger.info("WRITE MESSAGE");
                            logger.info("Write key" + request.getWriteKey() + ", Write value:"
                                    + request.getWriteValue());
                            break;
                        case READ:
                            logger.info("READ MESSAGE");
                            break;
                        case COMMIT:
                            logger.info("COMMIT MESSAGE");
                            break;
                        case ABORT:
                            logger.info("ABORT MESSAGE");
                            break;
                        }
                    }
                    else if (wrapper.getmessageclass() == LogPropMessage.class) {
                        LogPropMessage logPropMessage = (LogPropMessage) wrapper.getDeSerializedInnerMessage();
                        logger.info("Datacenter:" + this.dataCenterName + " get log prop from:"
                                + logPropMessage.getSourceDataCenterName());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class LogPropagationThread implements Runnable {
        private DataCenter fromDataCenter;
        private String toDataCenterName;

        // private ExecutorService executor;

        public LogPropagationThread(DataCenter fromDataCenter, String toDataCenterName) {
            super();
            this.fromDataCenter = fromDataCenter;
            this.toDataCenterName = toDataCenterName;
            // this.executor = Executors.newFixedThreadPool(dataCenterList.length);
        }

        public void run() {
            // TODO Auto-generated method stub
            while (true) {
                if (fromDataCenter.isAutoLogPropagation) {
                    try {
                        // Sleep for a specific RTT to simulate the latency
                        Thread.sleep(fromDataCenter.RTTLatencies.get(toDataCenterName));
                        // Send log to toDataCenter, message info including sourceDataCenter info and transactions that
                        // need to be propagated
                        fromDataCenter.sendLogPropagationMessage(toDataCenterName,
                                new LogPropMessage(fromDataCenter.dataCenterName,
                                        "Propagation"));
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                }
            }
        }

    }

    public static void main(String[] args) throws Exception {
        ArrayList<DataCenter> dataCenterList = new ArrayList<DataCenter>();
        String[] dataCenterNames = { "dc1", "dc2", "dc3" };
        int[] dataCenterLocations = { 1000, 1500, 4000 };
        for (int i = 0; i < dataCenterNames.length; i++) {
            dataCenterList.add(new DataCenter(dataCenterNames[i], 0, dataCenterLocations[i], dataCenterNames,
                    dataCenterLocations));
        }
        for (int i = 0; i < dataCenterNames.length; i++) {
            dataCenterList.get(i).generateRTTList();
            dataCenterList.get(i).generateCommitOffsets();
        }
        for (int i = 0; i < dataCenterList.size(); i++) {
            new Thread(dataCenterList.get(i)).start();
        }

        // DataCenter dc1 = new DataCenter("test1", 2);
        // DataCenter dc2 = new DataCenter("test2", 2);
        // (new Thread(dc1)).start();
        // (new Thread(dc2)).start();
    }
}
