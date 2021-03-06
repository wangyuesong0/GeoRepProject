//package helios.test;
//
//import helios.client.Client;
//import helios.datacenter.DataCenter;
//
///**
// * @Project: helios
// * @Title: LogPropagationTest.java
// * @Package helios.test
// * @Description: TODO
// * @author YuesongWang
// * @date Mar 17, 2016 12:45:58 AM
// * @version V1.0
// */
//public class LogScanAndPTScanWhenCommitDetectOverriteenTest {
//    public static void main(String[] args) throws Exception {
//        String[] dataCenterNames = { "dc0", "dc1" };
//        int[] dataCenterLocations = new int[] { 1000, 3000 };
//        DataCenter[] dataCenters = new DataCenter[2];
//        for (int i = 0; i < 2; i++) {
//            dataCenters[i] = new DataCenter("dc" + i, i * 2000, dataCenterNames, dataCenterLocations);
//        }
//
//        for (int i = 0; i < 2; i++) {
//            dataCenters[i].generateCommitOffsets();
//            dataCenters[i].generateRTTList();
//        }
//
//        for (int i = 0; i < 2; i++) {
//            new Thread(dataCenters[i]).start();
//        }
//
//        Client client = new Client("client1");
//        // new Thread(client).start();
//        client.sendBeginMessage("dc1");
//        Thread.sleep(1000);
//        client.sendWriteMessage("dc1", 0, "s", "b");
//        Thread.sleep(1000);
//        client.sendCommitMessage("dc1", 0);
//
//        // Do a read
//        client.sendBeginMessage("dc1");
//        client.sendReadMessage("dc1", 1, "s");
//
//        // Overrite
//        client.sendBeginMessage("dc1");
//        Thread.sleep(1000);
//        client.sendWriteMessage("dc1", 2, "s", "t");
//        Thread.sleep(1000);
//        client.sendCommitMessage("dc1", 2);
//        Thread.sleep(1000);
//
//        client.sendCommitMessage("dc1", 1);
//
//        // client.sendWriteMessage("dc1", 0, "shit", "fuck");
//        // Thread.sleep(1000);
//        // client.sendReadMessage("dc1", 0, "shit");
//        // Thread.sleep(1000);
//        // client.sendCommitMessage("dc1", 0);
//        // Thread.sleep(1000);
//    }
//}
