package helios.message.factory;

import helio.fakedb.DatastoreEntry;
import helios.message.CenterMessageType;
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
    public static CenterResponseMessage createBeginResponseMessage(String routingKey, long txnNum) {
        return new CenterResponseMessage(CenterMessageType.BEGIN, routingKey, txnNum);
    }

    public static CenterResponseMessage createReadResponseMessage(String routingKey, long txnNum,
            DatastoreEntry entry) {
        return new CenterResponseMessage(CenterMessageType.READ, routingKey, txnNum, entry);
    }
    
    public static CenterResponseMessage createAbortReponseMessage(String routingKey, long txnNum) {
        return new CenterResponseMessage(CenterMessageType.ABORT, routingKey, txnNum);
    }
}
