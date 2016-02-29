package helios.datacenter;

import helios.message.ClientRequestMessage;
import helios.message.MessageType;
import helios.message.MessageWrapper;
import helios.misc.Common;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
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
    private String dataCenterName;
    // Log queue
    private String fanoutQueueName;
    // Client message queue
    private String directQueueName;
    // Consumer for client message and log message
    private QueueingConsumer consumer;

    private int totalNumOfDataCenters;
    private long[][] rDict;
    private int[] RTTLatencies;
    private int[] commitOffsets;
    private PriorityQueue<Log> PTPool;
    private PriorityQueue<Log> EPTPool;
    private static Logger logger = Logger.getLogger(DataCenter.class);

    /**
     * Initialization
     * 
     * @param dataCenterName
     * @param totalNumOfDataCenters
     * @throws Exception
     */
    public DataCenter(String dataCenterName, int totalNumOfDataCenters) throws Exception {
        super();
        this.dataCenterName = dataCenterName;
        this.totalNumOfDataCenters = totalNumOfDataCenters;
        fanoutQueueName = this.dataCenterName + ".fanout.queue";
        directQueueName = this.dataCenterName + ".direct.queue";
        factory = new ConnectionFactory();
        factory.setHost(Common.MQ_HOST_NAME);
        connection = factory.newConnection();
        channel = connection.createChannel();
        rDict = new long[totalNumOfDataCenters][totalNumOfDataCenters];
        PTPool = new PriorityQueue<Log>();
        EPTPool = new PriorityQueue<Log>();
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
        channel.queueDeclare(directQueueName, false, false, false, null);
        channel.queueBind(directQueueName, Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME, this.dataCenterName);
        if (consumer == null) {
            consumer = new QueueingConsumer(channel);
        }
        channel.basicConsume(directQueueName, true, consumer);

    }

    /**
     * Bind datacenter to a fanout exchange for log propagation. Queue name: this.dataCenterName + ".fanout.queue"
     * Description: TODO
     * 
     * @throws IOException
     *             void
     * @throws InterruptedException
     * @throws ConsumerCancelledException
     * @throws ShutdownSignalException
     */
    public void bindToFanoutExchange() throws IOException, ShutdownSignalException, ConsumerCancelledException,
            InterruptedException {
        channel.exchangeDeclare(Common.FANOUT_EXCHANGE_NAME, "fanout");
        channel.queueDeclare(fanoutQueueName, false, false, false, null);
        channel.queueBind(fanoutQueueName, Common.FANOUT_EXCHANGE_NAME, "Whatever");
        if (consumer == null) {
            consumer = new QueueingConsumer(channel);
        }
        channel.basicConsume(fanoutQueueName, true, consumer);
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
//            this.bindToFanoutExchange(); 
        } catch (Exception e) {
            logger.error("Binding to exchange failed");
            System.exit(-1);
        }
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
                        if (request.getType().equals(MessageType.BEGIN)) {
                            logger.info("Begin message");
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        DataCenter dc = new DataCenter("test", 1);
        (new Thread(dc)).start();
    }
}
