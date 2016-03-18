package helios.datacenter;

import org.apache.log4j.BasicConfigurator;

/**
 * @Project: helios
 * @Title: DataCenter0.java
 * @Package helios.datacenter
 * @Description: TODO
 * @author YuesongWang
 * @date Mar 18, 2016 2:52:17 AM
 * @version V1.0
 */
public class DataCenter0 {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        String[] dataCenterNames = { "DataCenter0", "DataCenter1", "DataCenter2" };
        DataCenter dataCenter0 = new DataCenter("DataCenter0", dataCenterNames);
        
        dataCenter0.generateCommitOffsets();
        dataCenter0.generateRTTList();
        new Thread(dataCenter0).start();  
    }
}
