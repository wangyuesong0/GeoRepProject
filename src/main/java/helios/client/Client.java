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
    public void sendMessageToDataCenter(Message message) throws IOException {
        channel.basicPublish(Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME, message.getRoutingKey(), null,
                new MessageWrapper(Common.Serialize(message), message.getClass()).getSerializedMessage().getBytes());
    }

    /**
     * Send begin message
     * Description: TODO
     * 
     * @param routingKey
     *            void
     * @throws IOException
     */
    public void sendBeginMessage(String routingKey) throws IOException {
        logger.info("Client send begin message to datacenter:" + routingKey);
        sendMessageToDataCenter(ClientRequestMessageFactory.createBeginMessage(routingKey));
    }

    public static void main(String[] args) throws Exception {
        Client c = new Client();
        c.sendBeginMessage("test");
    }

    /**
     * Setup client message receiver
     */
    public void run() {
        // TODO Auto-generated method stub

    }

}
