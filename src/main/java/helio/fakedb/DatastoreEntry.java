package helio.fakedb;

/**
 * @Project: helios
 * @Title: DatastoreEntry.java
 * @Package helio.fakedb
 * @Description: TODO
 * @author YuesongWang
 * @date Mar 16, 2016 2:09:41 PM
 * @version V1.0
 */
public class DatastoreEntry {
    private String value;
    private long version;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

}
