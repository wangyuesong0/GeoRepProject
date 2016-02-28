package helios;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * @Project: helios
 * @Title: DataCenter.java
 * @Package helios
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 27, 2016 10:46:04 PM
 * @version V1.0
 */
public class DataCenter {
    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private String dataCenterName;
    private String queueName;

    public DataCenter(String dataCenterName) throws Exception {
        super();
        this.dataCenterName = dataCenterName;
        queueName = dataCenterName + ".queue";
        factory = new ConnectionFactory();
        factory.setHost(Common.MQ_HOST_NAME);
        connection = factory.newConnection();
        channel = connection.createChannel();
        bindToFanoutExchange();

    }

    public void bindToFanoutExchange() throws IOException {
        channel.exchangeDeclare(Common.FANOUT_EXCHANGE_NAME, "fanout");
        channel.queueDeclare(queueName, false, false, false, null);
        channel.queueBind(queueName, Common.FANOUT_EXCHANGE_NAME, "Whatever");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                    Envelope envelope, BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(message);
            }
        };
        channel.basicConsume(queueName, consumer);
    }
    
    

    @Override
    protected void finalize() throws Throwable {
        // TODO Auto-generated method stub
        super.finalize();
        this.channel.close();
        this.connection.close();
    }

    public static void main(String[] args) throws Exception {
       for(int i = 0; i < 5; i ++){
           new DataCenter(i+"fuck").bindToFanoutExchange();
       }
    }
}
