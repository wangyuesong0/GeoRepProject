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

    // This is used to update rDict even there is no logs in this message
    private long logPropTimestamp;

    public LogPropMessage() {
        super();
    }

    public long getLogPropTimestamp() {
        return logPropTimestamp;
    }

    public void setLogPropTimestamp(long logPropTimestamp) {
        this.logPropTimestamp = logPropTimestamp;
    }

    public LogPropMessage(String sourceDataCenterName, Log[] logs, long logPropTimestamp) {
        super();
        this.sourceDataCenterName = sourceDataCenterName;
        this.logs = logs;
        this.logPropTimestamp = logPropTimestamp;
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
