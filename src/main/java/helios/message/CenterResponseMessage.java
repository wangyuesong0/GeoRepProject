package helios.message;

/**
 * @Project: helios
 * @Title: CenterResponseMessage.java
 * @Package helios.message
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 28, 2016 2:08:24 PM
 * @version V1.0
 */
public class CenterResponseMessage extends Message {

    // Client's name
    private String routingKey;
    private long txnNum;

    /**
     * @param type
     * @param routingKey
     */
    public CenterResponseMessage() {
        super();
        // TODO Auto-generated constructor stub
    }

    public CenterResponseMessage(String routingKey, long txnNum) {
        super();
        this.routingKey = routingKey;
        this.txnNum = txnNum;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public long getTxnNum() {
        return txnNum;
    }

    public void setTxnNum(long txnNum) {
        this.txnNum = txnNum;
    }

    @Override
    public String toString() {
        return "CenterResponseMessage [routingKey=" + routingKey + ", txnNum=" + txnNum + "]";
    }

}
