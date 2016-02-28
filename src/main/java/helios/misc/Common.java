package helios.misc;

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
    public final static String FANOUT_EXCHANGE_NAME = "fanoutExchange";
    // For client send request to data center, with routing key
    public final static String DIRECT_EXCHANGE_NAME = "directExchange";
    public final static String MQ_HOST_NAME = "rabbithost";

    /**
     * Use json to serialize POJO message
     * Description: TODO
     * @param message
     * @return
     * String
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
