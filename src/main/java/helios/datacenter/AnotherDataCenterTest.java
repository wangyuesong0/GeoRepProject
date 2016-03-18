package helios.datacenter;

import java.util.HashMap;

import org.apache.log4j.BasicConfigurator;

/**
 * @Project: helios
 * @Title: DataCenter1.java
 * @Package helios.datacenter
 * @Description: TODO
 * @author YuesongWang
 * @date Mar 18, 2016 2:52:10 AM
 * @version V1.0
 */
public class AnotherDataCenterTest {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        String[] dataCenterNames = {"DataCenter2", "DataCenter3" };
        // int[] dataCenterLocations = new int[] { 0, 60 };
        HashMap<String, Integer> RTTLatencies = new HashMap<String, Integer>();
        RTTLatencies.put("DataCenter2", 40);
        RTTLatencies.put("DataCenter3", 0);

        HashMap<String, Integer> commitOffsets = new HashMap<String, Integer>();
        commitOffsets.put("DataCenter2", 0);
        commitOffsets.put("DataCenter3", 0);

        DataCenter dataCenter3 = new DataCenter("DataCenter3", dataCenterNames);
        dataCenter3.setRTTLatencies(RTTLatencies);
        dataCenter3.setCommitOffsets(commitOffsets);

        new Thread(dataCenter3).start();
    }
}
