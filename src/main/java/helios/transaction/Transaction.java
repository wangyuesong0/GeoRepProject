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
    protected HashMap<String, String> writeSet;
    protected HashMap<String, String> readSet;
    protected long timestamp;

    // Calculated kts to other datacenters
    protected HashMap<String, Integer> kts;

    public HashMap<String, String> getWriteSet() {
        return writeSet;
    }

    public void setWriteSet(HashMap<String, String> writeSet) {
        this.writeSet = writeSet;
    }

    public HashMap<String, String> getReadSet() {
        return readSet;
    }

    public void setReadSet(HashMap<String, String> readSet) {
        this.readSet = readSet;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public HashMap<String, Integer> getKts() {
        return kts;
    }

    public void setKts(HashMap<String, Integer> kts) {
        this.kts = kts;
    }

}
