
package helios.message;
/**  
 * @Project: helios
 * @Title: ClientRequestMessage.java
 * @Package helios.message
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 28, 2016 2:08:14 PM
 * @version V1.0  
 */
public class ClientRequestMessage extends Message {
    /**
     * @param type
     * @param routingKey
     */
    private String writeKey;
    private String readKey;
    private String writeValue;
    private Integer txnNum;
    
    /** 
     * Write Request
     * @param type
     * @param routingKey
     * @param writeKey
     * @param writeValue
     * @param txnNum
     */
    public ClientRequestMessage(MessageType type, String routingKey, String writeKey,
            String writeValue, Integer txnNum) {
        super(type, routingKey);
        this.writeKey = writeKey;
        this.writeValue = writeValue;
        this.txnNum = txnNum;
    }
    
    /**
     * Read Request
     * @param type
     * @param routingKey
     * @param readKey
     * @param txnNum
     */
    public ClientRequestMessage(MessageType type, String routingKey, String readKey, Integer txnNum) {
        super(type, routingKey);
        this.readKey = readKey;
        this.txnNum = txnNum;
    }
    
    /**
     * Begin Request
     * @param type
     * @param routingKey
     */
    public ClientRequestMessage(MessageType type, String routingKey) {
        super(type, routingKey);
    }
  
    /**
     * Commit/Abort Request
     * @param type
     * @param routingKey
     * @param txnNum
     */
    public ClientRequestMessage(MessageType type, String routingKey, Integer txnNum) {
        super(type, routingKey);
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

    public Integer getTxnNum() {
        return txnNum;
    }

    public void setTxnNum(Integer txnNum) {
        this.txnNum = txnNum;
    }
    
    
    
}
