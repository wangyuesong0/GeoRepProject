package helios.log;

import helios.transaction.Transaction;

import java.util.HashMap;
import java.util.List;

/**
 * @Project: helios
 * @Title: Log.java
 * @Package helios.datacenter
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 28, 2016 2:16:50 AM
 * @version V1.0
 */
public class Log implements Comparable<Log> {

    // Indicating whether this log has been processed by logScan
    private boolean isProcessed = false;

    // Contains one preparing or finished transaction
    private Transaction transaction;


    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public boolean isProcessed() {
        return isProcessed;
    }

    public void setProcessed(boolean isProcessed) {
        this.isProcessed = isProcessed;
    }

    public Log() {
        super();
    }

    public Log(Transaction transaction) {
        super();
        this.transaction = transaction;
    }

    @Override
    public String toString() {
        return "Log [isProcessed=" + isProcessed + ", transaction=" + transaction + "]";
    }

    public int compareTo(Log o) {
        // TODO Auto-generated method stub
        return this.getTransaction().compareTo(o.getTransaction());
    }

}
