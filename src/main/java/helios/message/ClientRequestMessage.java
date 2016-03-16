
package helios.message;
/**  
 * @Project: helios
 * @Title: ClientRequestMessage.java
 * @Package helios.message
 * @Description: Client Message sent to data center, including read, write, commit, abort, begin
 * @author YuesongWang
 * @date Feb 28, 2016 2:08:14 PM
 * @version V1.0  
 */
public class ClientRequestMessage extends Message {
    /**
     * @param type
     * @param routingKey
     */
    private ClientMessageType type;
    private String clientName;
    private String writeKey;
    private String readKey;
    private String writeValue;
    private long txnNum;
    
    private String routingKey;

    /** 
     * Write Request
     * @param type
     * @param clientName
     * @param routingKey
     * @param writeKey
     * @param writeValue
     * @param txnNum
     */
    public ClientRequestMessage(ClientMessageType type, String clientName, String routingKey, String writeKey,
            String writeValue, long txnNum) {
        super();
        this.type = type;
        this.routingKey = routingKey;
        this.clientName = clientName;
        this.writeKey = writeKey;
        this.writeValue = writeValue;
        this.txnNum = txnNum;
    }
    
    /**
     * Read Request
     * @param type
     * @param clientName
     * @param routingKey
     * @param readKey
     * @param txnNum
     */
    public ClientRequestMessage(ClientMessageType type, String clientName,String routingKey, String readKey, Integer txnNum) {
        super();
        this.type = type;
        this.routingKey = routingKey;
        this.clientName = clientName;
        this.readKey = readKey;
        this.txnNum = txnNum;
    }
    
    /**
     * Begin Request
     * @param type
     * @param clientName
     * @param routingKey
     */
    public ClientRequestMessage(ClientMessageType type, String clientName, String routingKey) {
        super();
        this.type = type;
        this.routingKey = routingKey;
        this.clientName = clientName;
    }
  
    /**
     * Commit/Abort Request
     * @param type
     * @param clientName
     * @param routingKey
     * @param txnNum
     */
    public ClientRequestMessage(ClientMessageType type,String clientName, String routingKey, Integer txnNum) {
        super();
        this.type = type;
        this.routingKey = routingKey;
        this.clientName = clientName;
        this.txnNum = txnNum;
    }

    public String getWriteKey() {
        return writeKey;
    }

    public void setWriteKey(String writeKey) {
        this.writeKey = writeKey;
    }

    public String getReadKey() {
        return readKey;
    }

    public void setReadKey(String readKey) {
        this.readKey = readKey;
    }

    public String getWriteValue() {
        return writeValue;
    }

    public void setWriteValue(String writeValue) {
        this.writeValue = writeValue;
    }

    public long getTxnNum() {
        return txnNum;
    }

    public void setTxnNum(long txnNum) {
        this.txnNum = txnNum;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public ClientMessageType getType() {
        return type;
    }

    public void setType(ClientMessageType type) {
        this.type = type;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }
    
    
    
    
    
    
    
}
