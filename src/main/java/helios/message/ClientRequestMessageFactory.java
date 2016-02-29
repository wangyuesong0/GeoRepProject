package helios.message;

/**
 * @Project: helios
 * @Title: ClientMessageFactory.java
 * @Package helios.message
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 28, 2016 3:09:35 PM
 * @version V1.0
 */
public class ClientRequestMessageFactory {
    public static Message createBeginMessage(String routingKey) {
        return new ClientRequestMessage(MessageType.BEGIN, routingKey);
    }

    public static Message createWriteMessage(Integer txnNum, String routingKey, String writeKey, String writeValue) {
        return new ClientRequestMessage(MessageType.WRITE, routingKey, writeKey, writeValue, txnNum);
    }

    public static Message createReadMessage(Integer txnNum, String routingKey, String readKey) {
        return new ClientRequestMessage(MessageType.READ, routingKey, readKey, txnNum);
    }

    public static Message createCommitMessage(Integer txnNum, String routingKey) {
        return new ClientRequestMessage(MessageType.COMMIT, routingKey, txnNum);
    }

    public static Message createAbortMessage(Integer txnNum, String routingKey) {
        return new ClientRequestMessage(MessageType.ABORT, routingKey, txnNum);
    }

}
