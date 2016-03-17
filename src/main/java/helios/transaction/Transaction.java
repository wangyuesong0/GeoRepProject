package helios.transaction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * @Project: heliosµ
 * @Title: Transaction.java
 * @Package helios
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 28, 2016 1:59:28 AM
 * @version V1.0
 */
public abstract class Transaction {
    protected HashMap<String, String> writeSet;
    protected HashMap<String, Long> readSet;
    protected long timestamp;
    protected long txnNum;
    // The client who send this transaction
    protected String clientName;
    // The data center who accepts this transaction
    protected String datacenterName;

    // Calculated kts to other datacenters
    protected HashMap<String, Long> kts;

    // Generate KTS to other data centers according to txn timestamp and commit offsets
    public void generateKTS(HashMap<String, Integer> commitOffsets) {
        Set<String> keySet = commitOffsets.keySet();
        Iterator<String> iterator = keySet.iterator();
        while (iterator.hasNext()) {
            String dataCentername = iterator.next();
            kts.put(dataCentername, commitOffsets.get(dataCentername) + this.timestamp);
        }
    }

    public Transaction(long txnNum, String clientName, String datacenterName) {
        super();
        this.txnNum = txnNum;
        this.clientName = clientName;
        this.datacenterName = datacenterName;
        this.writeSet = new HashMap<String, String>();
        this.readSet = new HashMap<String, Long>();
        this.kts = new HashMap<String, Long>();
    }

    public String getDatacenterName() {
        return datacenterName;
    }

    public void setDatacenterName(String datacenterName) {
        this.datacenterName = datacenterName;
    }

    public HashMap<String, String> getWriteSet() {
        return writeSet;
    }

    public void setWriteSet(HashMap<String, String> writeSet) {
        this.writeSet = writeSet;
    }

    public HashMap<String, Long> getReadSet() {
        return readSet;
    }

    public void setReadSet(HashMap<String, Long> readSet) {
        this.readSet = readSet;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public HashMap<String, Long> getKts() {
        return kts;
    }

    public void setKts(HashMap<String, Long> kts) {
        this.kts = kts;
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

}
