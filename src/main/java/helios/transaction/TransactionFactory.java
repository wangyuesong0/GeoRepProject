package helios.transaction;

import helios.datacenter.DataCenter;

/**
 * @Project: helios
 * @Title: TransactionFactory.java
 * @Package helios.transaction
 * @Description: TODO
 * @author YuesongWang
 * @date Mar 17, 2016 2:43:40 PM
 * @version V1.0
 */
public class TransactionFactory {
    public static Transaction createPreparingTransaction(long txnNum, String clientName, String datacenterName) {
        return new Transaction(TransactionType.PREPARING, txnNum, clientName, datacenterName);
    }

    public static Transaction createFinishedTransaction(long txnNum, String clientName, String datacenterName,
            boolean isCommited) {
        return new Transaction(TransactionType.FINISHED, txnNum, clientName, datacenterName);
    }

    public static Transaction createFinishedTransactionFromPreparingTransaction(Transaction preparingTransaction,
            boolean isCommited) {
        return new Transaction(preparingTransaction, isCommited);
    }
}
