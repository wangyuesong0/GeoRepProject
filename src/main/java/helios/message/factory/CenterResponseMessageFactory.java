package helios.message.factory;

import helios.message.CenterResponseMessage;

/**
 * @Project: helios
 * @Title: CenterResponseMessageFactory.java
 * @Package helios.message
 * @Description: TODO
 * @author YuesongWang
 * @date Mar 15, 2016 9:00:53 PM
 * @version V1.0
 */
public class CenterResponseMessageFactory {
    public static CenterResponseMessage createResponseMessage(String routingKey, long txnNum) {
        return new CenterResponseMessage(routingKey, txnNum);
    }
}
