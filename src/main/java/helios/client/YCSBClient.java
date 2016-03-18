package helios.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.TimeoutException;

import org.codehaus.jettison.json.JSONObject;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

/**
 * @Project: helios
 * @Title: YCSBClient.java
 * @Package helios.client
 * @Description: TODO
 * @author YuesongWang
 * @date Mar 17, 2016 3:14:37 AM
 * @version V1.0
 */
public class YCSBClient extends DB {

    Client client;

    @Override
    public void init() throws DBException {
        // TODO Auto-generated method stub
        super.init();
        Properties properties = getProperties();
        // String clientName = (String) properties.get("clientName");
        String clientName = "Client" + properties.getProperty("id");
        String datacenterName = "DataCenter" + properties.getProperty("id");
        
        try {
            this.client = new Client(clientName, datacenterName);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TimeoutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#delete(java.lang.String, java.lang.String)
     */
    @Override
    public Status delete(String arg0, String arg1) {
        // TODO Auto-generated method stub
        return Status.OK;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#insert(java.lang.String, java.lang.String, java.util.HashMap)
     */
    @Override
    public Status insert(String arg0, String arg1, HashMap<String, ByteIterator> arg2) {
        System.out.println("Insert");
        Long txnNum;
        try {
            txnNum = client.sendBeginMessage();
            client.sendWriteMessage(txnNum, arg1, new JSONObject(arg2).toString());
            boolean result = client.sendCommitMessage(txnNum);
            if (!result)
                return Status.ERROR;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return Status.OK;
    }
    
    

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#read(java.lang.String, java.lang.String, java.util.Set, java.util.HashMap)
     */
    @Override
    public Status read(String arg0, String arg1, Set<String> arg2, HashMap<String, ByteIterator> arg3) {
        System.out.println("Read");
        Long txnNum;
        try {
            txnNum = client.sendBeginMessage();
            String readValue = client.sendReadMessage(txnNum, arg1);
            boolean readResult = client.sendCommitMessage(txnNum);
            if (!readResult) {
                return Status.ERROR;
            }
            else {
                arg3.put(arg1, new StringByteIterator(readValue));
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return Status.OK;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#scan(java.lang.String, java.lang.String, int, java.util.Set, java.util.Vector)
     */
    @Override
    public Status scan(String table, String key, int recordCount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result) {
        System.out.println("Scan");
        Long txnNum;
        try {
            txnNum = client.sendBeginMessage();
            String readValue = client.sendReadMessage(txnNum, key);
            boolean readResult = client.sendCommitMessage(txnNum);
            if (!readResult) {
                return Status.ERROR;
            }
            else {
                HashMap<String, ByteIterator> oneRow = new HashMap<String, ByteIterator>();
                oneRow.put(key, new StringByteIterator(readValue));
                result.add(oneRow);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return Status.OK;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#update(java.lang.String, java.lang.String, java.util.HashMap)
     */
    @Override
    public Status update(String arg0, String arg1, HashMap<String, ByteIterator> arg2) {
        System.out.println("Update");
        Long txnNum;
        try {
            txnNum = client.sendBeginMessage();
            client.sendWriteMessage(txnNum, arg1, new JSONObject(arg2).toString());
            boolean result = client.sendCommitMessage(txnNum);
            if (!result)
                return Status.ERROR;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return Status.OK;
    }

}
