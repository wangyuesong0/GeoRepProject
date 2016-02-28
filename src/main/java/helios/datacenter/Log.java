
package helios.datacenter;
/**  
 * @Project: helios
 * @Title: Log.java
 * @Package helios.datacenter
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 28, 2016 2:16:50 AM
 * @version V1.0  
 */
public class Log implements Comparable<Log>{
    private long timestamp;

    public int compareTo(Log o) {
        // TODO Auto-generated method stub
        return this.timestamp > o.timestamp ? 1: -1;
    }
    
}
