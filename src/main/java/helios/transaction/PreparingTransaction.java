package helios.transaction;

import java.util.List;

/**
 * @Project: helios
 * @Title: PreparingTransaction.java
 * @Package helios.transaction
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 28, 2016 2:00:02 AM
 * @version V1.0
 */
public class PreparingTransaction extends Transaction {

    /**
     * @param txnNum
     * @param clientName
     * @param datacenterName
     */
    public PreparingTransaction(long txnNum, String clientName, String datacenterName) {
        super(txnNum, clientName, datacenterName);
        // TODO Auto-generated constructor stub
    }
  
}
