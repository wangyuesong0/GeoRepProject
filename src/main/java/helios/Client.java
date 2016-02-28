package helios;

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
public class Client {
    private final static String QUEUE_NAME = "hello";
    private static Logger logger = Logger.getLogger(Client.class);

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("rabbithost");
//        factory.setPort(15672);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String message = "Hello World";
        channel.basicPublish("fanoutExchange", "", null, message.getBytes());
    }
}
