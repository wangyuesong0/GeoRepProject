
package helios.transaction;

import java.util.HashMap;
import java.util.List;

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
    protected HashMap<String,String> writeSet;
    protected HashMap<String,String> readSet;
    protected long timestamp;
  //Calculated kts to other datacenters
    protected List<Long> kts;
    
}
