package helios.misc;

import helios.message.MessageWrapper;

import com.google.gson.Gson;

/**
 * @Project: helios
 * @Title: Common.java
 * @Package helios
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 28, 2016 1:37:59 AM
 * @version V1.0
 */
public class Common {
    // For log propagation
    public final static String LOG_DIRECT_EXCHANGE_NAME = "logPropDirectExchange";
    // For client send request to data center, with routing key
    public final static String CLIENT_REQUEST_DIRECT_EXCHANGE_NAME = "clientRequestDirectExchange";
    // public final static String DC_RESPONSE_DIRECT_EXCHANGE_NAME = "dcResponseDirectExchange";
    public final static String MQ_HOST_NAME = "rabbithost";

    // Client as a receiver
    public static String getDatacenterFeedbackMessageReceiverDirectQueue(String clientName) {
        return clientName + ".datacenterFeedbackMessageReceiver.direct.queue";
    }

    // Datacenter as receiver
    public static String getClientMessageReceiverDirectQueueName(String dataCenterName) {
        return dataCenterName + ".clientMessageReceiver.direct.queue";
    }

    public static String getDatacenterLogPropagationDirectQueueName(String dataCenterName) {
        return dataCenterName + ".logPropagation.direct.queue";
    }

    /**
     * Use json to serialize POJO message
     * Description: TODO
     * 
     * @param message
     * @return
     *         String
     */
    public static <T> String Serialize(T message)
    {
        Gson gson = new Gson();
        return gson.toJson(message, message.getClass());
    }

    @SuppressWarnings("rawtypes")
    public static <T> T Deserialize(String json, Class className)
    {
        Gson gson = new Gson();
        return (T) gson.fromJson(json, className);
    }

    public static <T> MessageWrapper CreateMessageWrapper(T message) {
        return new MessageWrapper(Common.Serialize(message), message.getClass());
    }

    public static Class GetClassfromString(String s) throws ClassNotFoundException
    {
        Class<?> cls = Class.forName(s);
        return cls;
    }
}
