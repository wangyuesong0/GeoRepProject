
package helios.optimalDataCenter;

import java.util.HashMap;

import helios.datacenter.DataCenter;

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
public class DataCenter2 {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        String[] dataCenterNames = { "DataCenter1", "DataCenter2", "DataCenter3" };
        // int[] dataCenterLocations = new int[] { 0, 60 };
        HashMap<String, Integer> RTTLatencies = new HashMap<String, Integer>();
        RTTLatencies.put("DataCenter1", 30);
        RTTLatencies.put("DataCenter2", 0);
        RTTLatencies.put("DataCenter3", 40);

        HashMap<String, Integer> commitOffsets = new HashMap<String, Integer>();
        commitOffsets.put("DataCenter1", 10);
        commitOffsets.put("DataCenter2", 0);
        commitOffsets.put("DataCenter3", 5);

        DataCenter dataCenter2 = new DataCenter("DataCenter2", dataCenterNames);
        dataCenter2.setRTTLatencies(RTTLatencies);
        dataCenter2.setCommitOffsets(commitOffsets);

        new Thread(dataCenter2).start();
    }
}
