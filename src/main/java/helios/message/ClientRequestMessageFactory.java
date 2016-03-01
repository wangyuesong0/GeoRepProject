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
    public static ClientRequestMessage createBeginMessage(String clientName, String routingKey) {
        return new ClientRequestMessage(MessageType.BEGIN, clientName, routingKey);
    }

    public static ClientRequestMessage createWriteMessage(String clientName, Integer txnNum, String routingKey, String writeKey,
            String writeValue) {
        return new ClientRequestMessage(MessageType.WRITE, clientName, routingKey, writeKey, writeValue, txnNum);
    }

    public static ClientRequestMessage createReadMessage(String clientName, Integer txnNum, String routingKey, String readKey) {
        return new ClientRequestMessage(MessageType.READ, clientName, routingKey, readKey, txnNum);
    }

    public static ClientRequestMessage createCommitMessage(String clientName, Integer txnNum, String routingKey) {
        return new ClientRequestMessage(MessageType.COMMIT, clientName, routingKey, txnNum);
    }

    public static ClientRequestMessage createAbortMessage(String clientName, Integer txnNum, String routingKey) {
        return new ClientRequestMessage(MessageType.ABORT, clientName, routingKey, txnNum);
    }

}
