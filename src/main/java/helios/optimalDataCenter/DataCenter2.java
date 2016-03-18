
package helios.optimalDataCenter;

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
        String[] dataCenterNames = { "DataCenter0", "DataCenter1", "DataCenter2" };
        int[] dataCenterLocations = new int[] { 0, 60, 120 };
        DataCenter dataCenter0 = new DataCenter("DataCenter2", 60, dataCenterNames, dataCenterLocations);
        dataCenter0.generateCommitOffsets();
        dataCenter0.generateRTTList();
        new Thread(dataCenter0).start();  
    }
}
