package helios.client;

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
        //
        // Connection connection = factory.newConnection();
        // Channel channel = connection.createChannel();
    }

    /**
     * Send begin message to a specific Datacenter by routingKey
     * Description: TODO
     * 
     * @param routingKey
     *            void
     */
    public void sendBeginMessage(String routingKey) {

    }

    // public void sendBeginMessage(String dest) throws IOException
    // {
    // ClientMessage msg = new ClientMessage(EXCHANGE_NAME, MessageType.BEGIN);
    // this.AddLogEntry("Sent Begin Request " + msg ,Level.INFO);
    // System.out.println("Sent Begin Request " + msg);
    // MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
    // channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
    // }

    public static void main(String[] args) throws Exception {

        // factory.setPort(15672);
//        Channel channel = connection.createChannel();
//        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
//        String message = "Hello World";
//        String fanoutMessage = "Fanout";
//        while (true) {
//            channel.basicPublish("directExchange", "1fuck", null, message.getBytes());
//            channel.basicPublish("fanoutExchange", "", null, fanoutMessage.getBytes());
//            Thread.sleep(5000);
//        }
    }

    /**
     * Setup client message receiver
     */
    public void run() {
        // TODO Auto-generated method stub

    }

}
