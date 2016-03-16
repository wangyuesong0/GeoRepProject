package helios.message;

import helio.fakedb.DatastoreEntry;

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

    private CenterMessageType type;
    // Client's name
    private String routingKey;
    private long txnNum;
    private DatastoreEntry readEntry;

    /**
     * @param type
     * @param routingKey
     */
    public CenterResponseMessage() {
        super();
        // TODO Auto-generated constructor stub
    }

    public CenterMessageType getType() {
        return type;
    }

    public void setType(CenterMessageType type) {
        this.type = type;
    }

    public DatastoreEntry getReadEntry() {
        return readEntry;
    }

    public void setReadEntry(DatastoreEntry readEntry) {
        this.readEntry = readEntry;
    }

    // Begin
    public CenterResponseMessage(CenterMessageType type, String routingKey, long txnNum) {
        super();
        this.type = type;
        this.routingKey = routingKey;
        this.txnNum = txnNum;
    }

    // Read
    public CenterResponseMessage(CenterMessageType type, String routingKey, long txnNum, DatastoreEntry readEntry) {
        super();
        this.type = type;
        this.routingKey = routingKey;
        this.txnNum = txnNum;
        this.readEntry = readEntry;
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
