package helios.client;

import helios.message.ClientRequestMessage;
import helios.message.ClientRequestMessageFactory;
import helios.message.Message;
import helios.message.MessageType;
import helios.message.MessageWrapper;
import helios.misc.Common;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

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

    public Client() throws IOException, TimeoutException {
        super();
        factory = new ConnectionFactory();
        // NEED TO SETUP HOSTS FILE
        factory.setHost("rabbithost");
        connection = factory.newConnection();
        channel = connection.createChannel();
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

    public static void main(String[] args) throws Exception {
        Client c = new Client();
        c.sendBeginMessage("test");
        c.sendWriteMessage("test", 1, "Hello", "Hello");
    }

    /**
     * Setup client message receiver
     */
    public void run() {
        // TODO Auto-generated method stub

    }

}
