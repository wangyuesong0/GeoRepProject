package helios.message;

/**
 * @Project: helios
 * @Title: LogPropMessage.java
 * @Package helios.message
 * @Description: Message send to fanout exchange for log propagation among all datacenters
 * @author YuesongWang
 * @date Feb 4, 2016 12:08:29 AM
 * @version V1.0
 */
public class LogPropMessage extends Message {
//    private MessageType type;
    private String sourceDataCenterName;
    private String info;
  
    
    public LogPropMessage(String dataCenterName, String info) {
        super();
        this.sourceDataCenterName = dataCenterName;
        this.info = info;
    }

   
    public String getDataCenterName() {
        return sourceDataCenterName;
    }


    public void setDataCenterName(String dataCenterName) {
        this.sourceDataCenterName = dataCenterName;
    }


    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

}
