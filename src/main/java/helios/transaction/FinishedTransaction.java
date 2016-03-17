package helios.transaction;

/**
 * @Project: helios
 * @Title: FinshedTransaction.java
 * @Package helios.transaction
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 28, 2016 2:00:13 AM
 * @version V1.0
 */
public class FinishedTransaction extends Transaction {

    // Commit or abort
    private boolean isCommitted;

    /**
     * @param txnNum
     * @param clientName
     * @param datacenterName
     */
    public FinishedTransaction(long txnNum, String clientName, String datacenterName) {
        super(txnNum, clientName, datacenterName);
        // TODO Auto-generated constructor stub
    }

    /**
     * Generate a finished transaction from preparing transaction
     * 
     * @param pre
     */
    public FinishedTransaction(PreparingTransaction pre) {
        super(pre.getTxnNum(), pre.getClientName(), pre.getDatacenterName());
        this.writeSet = pre.getWriteSet();
        this.readSet = pre.getReadSet();
        this.kts = pre.getKts();
    }

    public boolean isCommitted() {
        return isCommitted;
    }

    public void setCommitted(boolean isCommitted) {
        this.isCommitted = isCommitted;
    }

}
