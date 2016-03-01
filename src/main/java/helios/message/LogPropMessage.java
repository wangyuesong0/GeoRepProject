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
    private String dataCenterName;
    private String info;
  
    
    public LogPropMessage(String dataCenterName, String info) {
        super();
        this.dataCenterName = dataCenterName;
        this.info = info;
    }

   
    public String getDataCenterName() {
        return dataCenterName;
    }


    public void setDataCenterName(String dataCenterName) {
        this.dataCenterName = dataCenterName;
    }


    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

}
