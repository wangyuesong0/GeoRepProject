//package helios.test;
//
//import helios.log.Log;
//import helios.transaction.PreparingTransaction;
//import helios.transaction.Transaction;
//
//import java.util.Iterator;
//import java.util.PriorityQueue;
//import java.util.TreeSet;
//
///**
// * @Project: helios
// * @Title: PriorityQueueTest.java
// * @Package helios.test
// * @Description: TODO
// * @author YuesongWang
// * @date Mar 17, 2016 12:27:24 AM
// * @version V1.0
// */
//public class PriorityQueueTest {
//    public static void main(String[] args) {
//        TreeSet<Log> pq = new TreeSet<Log>();
//        for (int i = 0; i < 5; i++) {
////            Transaction t = new Transaction(1, "1", "1");
//            t.setTimestamp(i);
//            pq.add(new Log(t));
//        }
//        // int size = pq.size();
//        // for (int i = 0; i < size; i++) {
//        // System.out.println(pq.poll().getTransaction().getTimestamp());
//        // }
//        // Iterator<Log> iter = pq.iterator();
//        // while (iter.hasNext()) {
//        // Log l = iter.next();
//        // System.out.println(l.getTransaction().getTimestamp());
//        // }
//        for (Log l : pq) {
//            // Log l = iter.next();
//            System.out.println(l.getTransaction().getTimestamp());
//        }
//
//    }
//}
