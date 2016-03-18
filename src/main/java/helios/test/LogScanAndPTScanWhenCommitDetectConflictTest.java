package helios.test;

import java.util.HashMap;

import org.apache.log4j.BasicConfigurator;

import helios.client.Client;
import helios.datacenter.DataCenter;

/**
 * @Project: helios
 * @Title: LogPropagationTest.java
 * @Package helios.test
 * @Description: TODO
 * @author YuesongWang
 * @date Mar 17, 2016 12:45:58 AM
 * @version V1.0
 */
public class LogScanAndPTScanWhenCommitDetectConflictTest {
    public static void main(String[] args) throws Exception {
        String[] dataCenterNames = { "DataCenter1", "DataCenter2" };
        HashMap<String, Integer> RTTLatencies1 = new HashMap<String, Integer>();
        RTTLatencies1.put("DataCenter1", 0);
        RTTLatencies1.put("DataCenter2", 30);

        HashMap<String, Integer> commitOffsets1 = new HashMap<String, Integer>();
        commitOffsets1.put("DataCenter1", 0);
        commitOffsets1.put("DataCenter2", -10);

        DataCenter dataCenter1 = new DataCenter("DataCenter1", dataCenterNames);
        dataCenter1.setRTTLatencies(RTTLatencies1);
        dataCenter1.setCommitOffsets(commitOffsets1);

        HashMap<String, Integer> RTTLatencies2 = new HashMap<String, Integer>();
        RTTLatencies2.put("DataCenter1", 30);
        RTTLatencies2.put("DataCenter2", 0);

        HashMap<String, Integer> commitOffsets2 = new HashMap<String, Integer>();
        commitOffsets2.put("DataCenter1", 10);
        commitOffsets2.put("DataCenter2", 0);

        DataCenter dataCenter2 = new DataCenter("DataCenter2", dataCenterNames);
        dataCenter2.setRTTLatencies(RTTLatencies2);
        dataCenter2.setCommitOffsets(commitOffsets2);
        
        new Thread(dataCenter1).start();
        new Thread(dataCenter2).start();

        Thread.sleep(1000);
        Client client = new Client("client1", "DataCenter1");
        // new Thread(client).start();
        client.sendBeginMessage();
        Thread.sleep(1000);
        client.sendWriteMessage(0, "s", "b");
        Thread.sleep(1000);

        client.sendBeginMessage();
        Thread.sleep(1000);
        client.sendWriteMessage(1, "s", "t");
        Thread.sleep(1000);
        client.sendCommitMessage(0);
//
//        client.sendReadMessage(2, "s");
//        Thread.sleep(1000);
//        client.sendCommitMessage(2);

        // client.sendWriteMessage("dc1", 0, "shit", "fuck");
        // Thread.sleep(1000);
        // client.sendReadMessage("dc1", 0, "shit");
        // Thread.sleep(1000);
        // client.sendCommitMessage("dc1", 0);
        // Thread.sleep(1000);
    }
}
