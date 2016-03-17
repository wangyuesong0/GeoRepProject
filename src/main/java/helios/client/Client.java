package helios.client;

import helios.message.CenterMessageType;
import helios.message.CenterResponseMessage;
import helios.message.ClientRequestMessage;
import helios.message.MessageWrapper;
import helios.message.factory.ClientRequestMessageFactory;
import helios.misc.Common;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * @Project: helios
 * @Title: Client.java
 * @Package helios
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 27, 2016 10:46:10 PM
 * @version V1.0
 */
public class Client implements Runnable {
    private static Logger logger = Logger.getLogger(Client.class);
    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    // UNIQUE routing key
    private String clientName;
    private String datacenterFeedbackMessageReceiverDirectQueueName;
    private QueueingConsumer consumer;

    public Client(String clientName) throws IOException, TimeoutException {
        super();
        this.clientName = clientName;
        factory = new ConnectionFactory();
        // NEED TO SETUP HOSTS FILE
        factory.setHost(Common.MQ_HOST_NAME);
        connection = factory.newConnection();
        channel = connection.createChannel();
        this.datacenterFeedbackMessageReceiverDirectQueueName = Common
                .getDatacenterFeedbackMessageReceiverDirectQueue(clientName);

    }

    public void bindToDatacenterMessageReceiverQueue() throws Exception {
        channel.exchangeDeclare(Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME, "direct");
        channel.queueDeclare(this.datacenterFeedbackMessageReceiverDirectQueueName, false, false, false, null);
        // 使用datacenterFeedbackMessageReceiverDirectQueueName这个Queue绑定到Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME这个exchange上，routing
        // key为clientName
        channel.queueBind(this.datacenterFeedbackMessageReceiverDirectQueueName,
                Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME,
                this.clientName);
        if (consumer == null) {
            consumer = new QueueingConsumer(channel);
        }
        channel.basicConsume(this.datacenterFeedbackMessageReceiverDirectQueueName, true, consumer);
    }

    /**
     * Send message to DC, message contains a routing key
     * Description: TODO
     * 
     * @param message
     * @throws IOException
     *             void
     */
    public void sendMessageToDataCenter(ClientRequestMessage message) throws IOException {
        channel.basicPublish(Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME, message.getRoutingKey(), null,
                new MessageWrapper(Common.Serialize(message), message.getClass()).getSerializedMessage().getBytes());
    }

    /**
     * Description: Send begin message
     * 
     * @param routingKey
     *            void
     * @throws IOException
     */
    public void sendBeginMessage(String routingKey) throws IOException {
        logger.info("Client send begin message to datacenter:" + routingKey);
        sendMessageToDataCenter(ClientRequestMessageFactory.createBeginMessage(this.clientName, routingKey));
    }

    /**
     * 
     * Description: Send read message
     * 
     * @param routingKey
     * @param txnNum
     * @param readKey
     * @throws IOException
     *             void
     */
    public void sendReadMessage(String routingKey, int txnNum, String readKey) throws IOException {
        logger.info("Client send read message to datacenter:" + routingKey);
        sendMessageToDataCenter(ClientRequestMessageFactory.createReadMessage(this.clientName, txnNum, routingKey,
                readKey));
    }

    /**
     * Description: Send write message
     * 
     * @param routingKey
     * @param txnNum
     * @param writeKey
     * @param writeValue
     * @throws IOException
     *             void
     */
    public void sendWriteMessage(String routingKey, int txnNum, String writeKey, String writeValue) throws IOException {
        logger.info("Client send write message to datacenter:" + routingKey);
        sendMessageToDataCenter(ClientRequestMessageFactory
                .createWriteMessage(this.clientName, txnNum, routingKey, writeKey, writeValue));
    }

    /**
     * 
     * Description: Send commit message
     * 
     * @param routingKey
     * @param txnNum
     * @throws IOException
     *             void
     */
    public void sendCommitMessage(String routingKey, int txnNum) throws IOException {
        logger.info("Client send commit message to datacenter:" + routingKey);
        sendMessageToDataCenter(ClientRequestMessageFactory.createCommitMessage(this.clientName, txnNum, routingKey));
    }

    /**
     * 
     * Description: Send abort message
     * 
     * @param routingKey
     * @param txnNum
     * @throws IOException
     *             void
     */
    public void sendAbortMessage(String routingKey, int txnNum) throws IOException {
        logger.info("Client send abort message to datacenter:" + routingKey);
        sendMessageToDataCenter(ClientRequestMessageFactory.createAbortMessage(this.clientName, txnNum, routingKey));
    }

    // public static void main(String[] args) throws Exception {
    // Client c = new Client();
    // c.sendBeginMessage("test");
    // c.sendWriteMessage("test", 1, "Hello", "Hello");
    // }

    /**
     * Setup client message receiver
     */
    public void run() {
        // TODO Auto-generated method stub
        MessageWrapper wrapper = null;
        QueueingConsumer.Delivery delivery;
        logger.info("Client: " + this.clientName + " is Running");
        try {
            this.bindToDatacenterMessageReceiverQueue();
        } catch (Exception e) {
            logger.error("Client: " + this.clientName + " bind to client request exchange failed");
            e.printStackTrace();
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
                    if (wrapper.getmessageclass() == CenterResponseMessage.class) {
                        CenterResponseMessage reponseMessage = (CenterResponseMessage) wrapper
                                .getDeSerializedInnerMessage();
                        CenterMessageType t = reponseMessage.getType();
                        logger.info("Client:" + this.clientName + " get" + t + " message from datacenter");
                        logger.info(reponseMessage);
                        switch (t) {
                        case BEGIN:
                            logger.info("");
                            break;
                        case READ:
                            // Need to handle null
                            break;
                        default:
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        Client a = new Client("shit");
        new Thread(a).start();
        a.sendBeginMessage("dc1");

    }

}
