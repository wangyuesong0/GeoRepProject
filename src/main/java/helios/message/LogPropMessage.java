package helios.message;

import helios.log.Log;

import java.util.List;
import java.util.PriorityQueue;

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
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    // private MessageType type;
    private String sourceDataCenterName;
    private Log[] logs;

    public LogPropMessage() {
        super();
    }

    public LogPropMessage(String sourceDataCenterName, Log[] logs) {
        super();
        this.sourceDataCenterName = sourceDataCenterName;
        this.logs = logs;
    }

    public String getSourceDataCenterName() {
        return sourceDataCenterName;
    }

    public void setSourceDataCenterName(String sourceDataCenterName) {
        this.sourceDataCenterName = sourceDataCenterName;
    }

    public Log[] getLogs() {
        return logs;
    }

    public void setLogs(Log[] logs) {
        this.logs = logs;
    }

}
