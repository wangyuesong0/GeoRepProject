package helios.transaction;

/**
 * @Project: helios
 * @Title: TransactionDetail.java
 * @Package helios.transaction
 * @Description: To be kept in data center local, when commit it will transfer to Transaction
 * @author YuesongWang
 * @date Mar 15, 2016 5:55:53 PM
 * @version V1.0
 */
public class TransactionDetail {
    private int txnNum;
    private Transaction transaction;
    private String clientName;

    public int getTxnNum() {
        return txnNum;
    }

    public void setTxnNum(int txnNum) {
        this.txnNum = txnNum;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

}
