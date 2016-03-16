//package helios.log;
//
//import helios.transaction.Transaction;
//
//import java.util.HashMap;
//import java.util.List;
//
///**
// * @Project: helios
// * @Title: Log.java
// * @Package helios.datacenter
// * @Description: TODO
// * @author YuesongWang
// * @date Feb 28, 2016 2:16:50 AM
// * @version V1.0
// */
//public class Log implements Comparable<Log> {
//    private long timestamp;
//
//    private List<Transaction> transactions;
//
//    public int compareTo(Log o) {
//        // TODO Auto-generated method stub
//        return this.timestamp > o.timestamp ? 1 : -1;
//    }
//
//    public long getTimestamp() {
//        return timestamp;
//    }
//
//    public void setTimestamp(long timestamp) {
//        this.timestamp = timestamp;
//    }
//
//    public List<Transaction> getTransactions() {
//        return transactions;
//    }
//
//    public void setTransactions(List<Transaction> transactions) {
//        this.transactions = transactions;
//    }
//
//}
