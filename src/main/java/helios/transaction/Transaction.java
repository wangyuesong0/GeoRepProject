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
public class Transaction implements Comparable<Transaction> {
    private TransactionType type;
    private HashMap<String, String> writeSet;
    private HashMap<String, Long> readSet;
    private long txnNum;
    // The client who send this transaction
    private String clientName;
    // The data center who accepts this transaction
    private String datacenterName;

    // Below need to be filled when committed
    private long timestamp;
    private HashMap<String, Long> kts;

    // Only for finished transaction
    private boolean isCommitted;

    // Generate KTS to other data centers according to txn timestamp and commit offsets
    public void generateKTS(HashMap<String, Integer> commitOffsets) {
        Set<String> keySet = commitOffsets.keySet();
        Iterator<String> iterator = keySet.iterator();
        while (iterator.hasNext()) {
            String dataCentername = iterator.next();
            kts.put(dataCentername, commitOffsets.get(dataCentername) + this.timestamp);
        }
    }

    // For creating a preparing transaction
    public Transaction(TransactionType type, long txnNum, String clientName, String datacenterName) {
        super();
        this.type = type;
        this.txnNum = txnNum;
        this.clientName = clientName;
        this.datacenterName = datacenterName;
        this.writeSet = new HashMap<String, String>();
        this.readSet = new HashMap<String, Long>();
        this.kts = new HashMap<String, Long>();
    }

    // For creating a finished transaction
    public Transaction(TransactionType type, long txnNum, String clientName, String datacenterName, boolean isCommited) {
        super();
        this.type = type;
        this.txnNum = txnNum;
        this.clientName = clientName;
        this.datacenterName = datacenterName;
        this.writeSet = new HashMap<String, String>();
        this.readSet = new HashMap<String, Long>();
        this.kts = new HashMap<String, Long>();
        this.isCommitted = isCommited;
    }

    // For creating a finished transaction from preparing transaction
    public Transaction(Transaction preparingTransaction, boolean isCommited) {
        super();
        this.type = TransactionType.FINISHED;
        this.txnNum = preparingTransaction.txnNum;
        this.clientName = preparingTransaction.clientName;
        this.datacenterName = preparingTransaction.datacenterName;
        this.writeSet = preparingTransaction.writeSet;
        this.readSet = preparingTransaction.readSet;
        this.kts = preparingTransaction.kts;
    }

    public Transaction() {
        super();
    }

    public TransactionType getType() {
        return type;
    }

    public void setType(TransactionType type) {
        this.type = type;
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

    public String getDatacenterName() {
        return datacenterName;
    }

    public void setDatacenterName(String datacenterName) {
        this.datacenterName = datacenterName;
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

    public boolean isCommitted() {
        return isCommitted;
    }

    public void setCommitted(boolean isCommitted) {
        this.isCommitted = isCommitted;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(Transaction o) {
        return timestamp > o.getTimestamp() ? 1 : -1;
    }
}
