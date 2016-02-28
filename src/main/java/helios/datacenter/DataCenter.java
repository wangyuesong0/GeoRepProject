package helios.datacenter;

import helios.misc.Common;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

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
public class DataCenter implements Runnable{
    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private String dataCenterName;
    //Log queue
    private String fanoutQueueName;
    //Client message queue
    private String directQueueName;
    private int totalNumOfDataCenters;
    private long[][] rDict;
    private int[] RTTLatencies;
    private int[] commitOffsets;
    private PriorityQueue<Log> PTPool;
    private PriorityQueue<Log> EPTPool;
    private static Logger logger = Logger.getLogger(DataCenter.class);
    /**
     * Initialization
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
        bindToFanoutExchange();
        bindToClientExchange();
    }
    /**
     * Bind datacenter to a direct exchange to receive client message.
     * Queue name: this.dataCenterName + ".direct.queue", Routing Key: this.dataCenterName
     * Description: TODO
     * void
     * @throws Exception 
     */
    public void bindToClientExchange() throws Exception{
        channel.exchangeDeclare(Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME, "direct");
        channel.queueDeclare(directQueueName, false, false, false, null);
        channel.queueBind(directQueueName, Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME, this.dataCenterName);
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                    Envelope envelope, BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                logger.info("Client Message:" + message);
            }
        };
        channel.basicConsume(directQueueName, consumer);
    }

    /**
     * Bind datacenter to a fanout exchange for log propagation. Queue name: this.dataCenterName + ".fanout.queue"
     * Description: TODO
     * @throws IOException
     * void
     */
    public void bindToFanoutExchange() throws IOException {
        channel.exchangeDeclare(Common.FANOUT_EXCHANGE_NAME, "fanout");
        channel.queueDeclare(fanoutQueueName, false, false, false, null);
        channel.queueBind(fanoutQueueName, Common.FANOUT_EXCHANGE_NAME, "Whatever");
       
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                    Envelope envelope, BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                logger.info("Log Propagation:" + message);
            }
        };
        channel.basicConsume(fanoutQueueName, consumer);
    }
    
    
    private long getCurrentTimestamp(){
       return System.currentTimeMillis() / 1000L;
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
           DataCenter d = new DataCenter(i+"fuck",5);
           d.bindToFanoutExchange();
           d.bindToClientExchange();
       }
    }
    /**
     * Datacenter run method, wait for incoming client request
     */
    public void run() {
        logger.info("Data Center: " + this.dataCenterName + " is Running");
        
    }
}
