package helio.fakedb;

import java.util.Date;
import java.util.HashMap;

/**
 * @Project: helios
 * @Title: Datastore.java
 * @Package helio.fakedb
 * @Description: TODO
 * @author YuesongWang
 * @date Mar 16, 2016 2:09:23 PM
 * @version V1.0
 */
public class Datastore {
    private HashMap<String, DatastoreEntry> values;

//    public HashMap<Long, DatastoreEntry> getValues() {
//        return values;
//    }
//
//    public void setValues(HashMap<Long, DatastoreEntry> values) {
//        this.values = values;
//    }

    public void writeValue(String key, String value) {
        DatastoreEntry entry = new DatastoreEntry();
        // Version timestamp
        entry.setVersion(new Date().getTime());
        entry.setValue(value);
        values.put(key, entry);
    }

    public DatastoreEntry readValue(String key) {
        return values.get(key);
    }
}
