package helios.datacenter;

import helios.log.Log;
import helios.message.ClientMessageType;
import helios.message.ClientRequestMessage;
import helios.message.LogPropMessage;
import helios.message.MessageWrapper;
import helios.misc.Common;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

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

    private DataCenter[] dataCenterList;

    // Use location to calculated the simulated RTT between datacenters;
    private int location;
    // Consumer for client message and log message
    private QueueingConsumer consumer;

    private int totalNumOfDataCenters;
    // Now I just understand one row's function... Just use it when periodically
    private long[] rDict;

    // To simulate log propagation latencies
    private int[] RTTLatencies;
    // Each center is assigned a commitOffset to other data center, used to decide whether to commit or abort
    private int[] commitOffsets;

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
    public DataCenter(String dataCenterName, int dataCenterIndex, int location, DataCenter[] dataCenterList)
            throws Exception {
        super();
        this.dataCenterList = dataCenterList;
        this.dataCenterIndex = dataCenterIndex;
        this.location = location;
        this.dataCenterName = dataCenterName;
        this.totalNumOfDataCenters = dataCenterList.length;
        // fanoutQueueName = this.dataCenterName + ".fanout.queue";
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
        for (int i = 0; i < dataCenterList.length; i++) {
            this.RTTLatencies[i] = Math.abs(this.location - dataCenterList[i].location);
        }
    }

    // Temporarily use 0 as all commit offsets
    public void generateCommitOffsets() {
        for (int i = 0; i < dataCenterList.length; i++) {
            this.commitOffsets[i] = 0;
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
        channel.exchangeDeclare(Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME, "direct");
        channel.queueDeclare(this.clientMessageDirectQueueName, false, false, false, null);
        // 使用logPropagationDirectQueueName这个Queue绑定到Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME这个exchange上，routing
        // key为dataCenterName
        channel.queueBind(this.logPropagationDirectQueueName, Common.LOG_DIRECT_EXCHANGE_NAME,
                this.dataCenterName);
        if (consumer == null) {
            consumer = new QueueingConsumer(channel);
        }
        channel.basicConsume(clientMessageDirectQueueName, true, consumer);
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
            this.bindToLogExchange();
        } catch (Exception e) {
            logger.error("Binding to client/log exchange failed");
            System.exit(-1);
        }
        // Start Log propagation
        new Thread(new LogPropagationThread(this)).start();

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
                        logger.info("Log prop from:" + logPropMessage.getDataCenterName());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class LogPropagationThread implements Runnable {
        private DataCenter dc;

        public LogPropagationThread(DataCenter dc) {
            super();
            this.dc = dc;
        }

        public void run() {
            // TODO Auto-generated method stub
            while (true) {
                if (dc.isAutoLogPropagation) {
                    try {
                        dc.sendLogPropagationMessage(new LogPropMessage(dc.dataCenterName, "Propagation"));
                        Thread.sleep(1000);
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
        DataCenter dc1 = new DataCenter("test1", 2);
        DataCenter dc2 = new DataCenter("test2", 2);
        (new Thread(dc1)).start();
        (new Thread(dc2)).start();
    }
}
