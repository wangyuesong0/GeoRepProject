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

    // Contains one preparing or finished transaction
    private Transaction transaction;

    public boolean isPreparing() {
        return transaction.getClass().getName().equals("PreparingTransaction");
    }

    public boolean isFinished() {
        return transaction.getClass().getName().equals("FinishedTransaction");
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public Log(Transaction transaction) {
        super();
        this.transaction = transaction;
    }

    public int compareTo(Log o) {
        // TODO Auto-generated method stub
        return this.getTransaction().getTimestamp() > o.getTransaction().getTimestamp() ? 1 : -1;
    }

}
