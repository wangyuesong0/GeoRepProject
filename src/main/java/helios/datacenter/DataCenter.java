package helios.datacenter;

import helio.fakedb.Datastore;
import helio.fakedb.DatastoreEntry;
import helios.log.Log;
import helios.message.CenterResponseMessage;
import helios.message.ClientMessageType;
import helios.message.ClientRequestMessage;
import helios.message.LogPropMessage;
import helios.message.MessageWrapper;
import helios.message.factory.CenterResponseMessageFactory;
import helios.misc.Common;
import helios.transaction.FinishedTransaction;
import helios.transaction.PreparingTransaction;
import helios.transaction.Transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;

import javax.sql.PooledConnection;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * @Project: helios
 * @Title: DataCenter.java
 * @Package helios
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 27, 2016 10:46:04 PM
 * @version V1.0
 */
public class DataCenter implements Runnable {
    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;

    private Datastore datastore;

    // UNIQUE routing key
    private String dataCenterName;

    // Queue for receive client message
    private String clientMessageDirectQueueName;

    // Queue for log propagation
    private String logPropagationDirectQueueName;

    // Use location to calculated the simulated RTT between datacenters;
    private int location;

    // Consumer for client message and log message
    private QueueingConsumer consumer;

    // Need to know all data center names for routing
    private String[] dataCenterNames;

    // Need to know all data center locations for generating fake RTT
    private int[] dataCenterLocations;

    // Just for simplicity
    private int totalNumOfDataCenters;

    // Now I just understand one row's function... Just use it
    private long[] rDict;

    // To simulate log propagation latencies, generate by subtracting location attribute
    private HashMap<String, Integer> RTTLatencies;

    // Each center is assigned a commitOffset to other data center, used to decide whether to commit or abort, now just
    // set to 0
    private HashMap<String, Integer> commitOffsets;

    // Local transaction pool
    private PriorityQueue<PreparingTransaction> PTPool;

    // External transaction pool
    private PriorityQueue<PreparingTransaction> EPTPool;

    // Shared log pool
    private PriorityQueue<Log> logs;

    // To keep user's transaction that haven't been committed by user. TxnNum -> TransactionDetail
    // Will generate a transaction in PTPool and a log in logs when user commit it
    private HashMap<Long, PreparingTransaction> uncommitedTxnDetails;

    // Log propagation switch
    private boolean isAutoLogPropagation = true;

    // Transaction count generator
    public long txnNumGenerator;

    private static Logger logger = Logger.getLogger(DataCenter.class);

    /**
     * Initialization
     * 
     * @param dataCenterName
     * @param totalNumOfDataCenters
     * @throws Exception
     */
    public DataCenter(String dataCenterName, int dataCenterIndex, int location, String[] dataCenterNames,
            int[] dataCenterLocations)
            throws Exception {
        super();

        this.dataCenterName = dataCenterName;
        this.location = location;
        this.dataCenterNames = dataCenterNames;
        this.dataCenterLocations = dataCenterLocations;

        this.totalNumOfDataCenters = dataCenterNames.length;
        this.RTTLatencies = new HashMap<String, Integer>();
        this.commitOffsets = new HashMap<String, Integer>();

        clientMessageDirectQueueName = Common.getClientMessageReceiverDirectQueueName(dataCenterName);
        logPropagationDirectQueueName = Common.getDatacenterLogPropagationDirectQueueName(dataCenterName);

        datastore = new Datastore();
        factory = new ConnectionFactory();
        factory.setHost(Common.MQ_HOST_NAME);
        connection = factory.newConnection();
        channel = connection.createChannel();
        rDict = new long[totalNumOfDataCenters];
        PTPool = new PriorityQueue<PreparingTransaction>();
        EPTPool = new PriorityQueue<PreparingTransaction>();
        uncommitedTxnDetails = new HashMap<Long, PreparingTransaction>();
    }

    // Generate RTT list based on location setting of data centers
    public void generateRTTList() {
        for (int i = 0; i < dataCenterLocations.length; i++) {
            // this.RTTLatencies[i] = Math.abs(this.location - dataCenterLocations[i]);
            this.RTTLatencies.put(dataCenterNames[i], Math.abs(this.location - dataCenterLocations[i]));
        }
    }

    // Temporarily use 0 as all commit offsets
    public void generateCommitOffsets() {
        for (int i = 0; i < dataCenterNames.length; i++) {
            this.commitOffsets.put(dataCenterNames[i], 0);
        }
    }

    /**
     * Bind datacenter to a direct exchange to receive log propgation message.
     * Queue name: this.dataCenterName + "log.direct.queue";, Routing Key: this.dataCenterName
     * Description: TODO
     * void
     * 
     * @throws Exception
     */
    public void bindToLogExchange() throws Exception {
        channel.exchangeDeclare(Common.LOG_DIRECT_EXCHANGE_NAME, "direct");
        channel.queueDeclare(this.logPropagationDirectQueueName, false, false, false, null);
        // 使用logPropagationDirectQueueName这个Queue绑定到Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME这个exchange上，routing
        // key为dataCenterName
        channel.queueBind(this.logPropagationDirectQueueName, Common.LOG_DIRECT_EXCHANGE_NAME,
                this.dataCenterName);
        if (consumer == null) {
            consumer = new QueueingConsumer(channel);
        }
        channel.basicConsume(this.logPropagationDirectQueueName, true, consumer);
    }

    /**
     * Bind datacenter to a direct exchange to receive client message.
     * Queue name: this.dataCenterName + ".direct.queue", Routing Key: this.dataCenterName
     * Description: TODO
     * void
     * 
     * @throws Exception
     */
    public void bindToClientExchange() throws Exception {
        channel.exchangeDeclare(Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME, "direct");
        channel.queueDeclare(this.clientMessageDirectQueueName, false, false, false, null);

        // 使用clientMessageDirectQueueName这个Queue绑定到Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME这个exchange上，routing
        // key为dataCenterName
        channel.queueBind(this.clientMessageDirectQueueName, Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME,
                this.dataCenterName);
        if (consumer == null) {
            consumer = new QueueingConsumer(channel);
        }
        channel.basicConsume(clientMessageDirectQueueName, true, consumer);

    }

    /**
     * 
     * Description: Send log propagation message, routing key is center's name
     * 
     * @param message
     * @throws IOException
     *             void
     */
    public void sendLogPropagationMessage(String routingKey, LogPropMessage message) throws IOException {
        channel.basicPublish(Common.LOG_DIRECT_EXCHANGE_NAME, routingKey, null,
                new MessageWrapper(Common.Serialize(message), message.getClass()).getSerializedMessage().getBytes());
    }

    /**
     * 
     * Description: Send center response message, routing key is client's name
     * 
     * @param message
     * @throws IOException
     *             void
     */
    public void sendCenterResponseMessage(CenterResponseMessage message) throws IOException {
        channel.basicPublish(Common.CLIENT_REQUEST_DIRECT_EXCHANGE_NAME, message.getRoutingKey(), null,
                new MessageWrapper(Common.Serialize(message), message.getClass()).getSerializedMessage().getBytes());
    }

    @Override
    protected void finalize() throws Throwable {
        // TODO Auto-generated method stub
        super.finalize();
        this.channel.close();
        this.connection.close();
    }

    /**
     * Datacenter run method, wait for incoming client request
     */
    public void run() {
        MessageWrapper wrapper = null;
        QueueingConsumer.Delivery delivery;
        logger.info("Data Center: " + this.dataCenterName + " is Running");
        try {
            this.bindToClientExchange();
        } catch (Exception e) {
            logger.error(this.dataCenterName + " binding to client exchange failed");
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            this.bindToLogExchange();
        } catch (Exception e1) {
            logger.error(this.dataCenterName + " binding to log exchange failed");
            e1.printStackTrace();
            System.exit(-1);
        }
        // Start Log propagation
        for (int i = 0; i < dataCenterNames.length; i++) {
            if (!dataCenterNames[i].equals(this.dataCenterName))
                // Propgate log to datacenter other than itself
                new Thread(new LogPropagationThread(this, dataCenterNames[i])).start();
        }
        // Receive Log
        try {
            while (true) {
                delivery = consumer.nextDelivery();
                if (delivery != null) {
                    String msg = new String(delivery.getBody());
                    wrapper = MessageWrapper.getDeSerializedMessage(msg);
                }
                if (wrapper != null) {
                    if (wrapper.getmessageclass() == ClientRequestMessage.class) {
                        ClientRequestMessage request = (ClientRequestMessage) wrapper.getDeSerializedInnerMessage();
                        // System.out.println("BEGIN REQUEST");
                        // logger.info("Client message received");
                        ClientMessageType t = request.getType();
                        switch (t) {
                        case BEGIN:
                            logger.info("Receive BEGIN message from client:" + request.getClientName());
                            this.handleBeginMessage(request);
                            break;
                        case WRITE:
                            logger.info("Receive WRITE mssage from client:" + request.getClientName());
                            logger.info("Txn:" + request.getTxnNum() + ",Write key:" + request.getWriteKey()
                                    + ",Write value:"
                                    + request.getWriteValue());
                            this.handleWriteMessage(request);
                            break;
                        case READ:
                            logger.info("Receive READ mssage from client:" + request.getClientName());
                            logger.info("Txn:" + request.getTxnNum() + ",Read key" + request.getReadKey());
                            this.handleReadMessage(request);
                            break;
                        case COMMIT:
                            logger.info("Receive COMMIT mssage from client:" + request.getClientName());
                            logger.info("Txn: " + request.getTxnNum());
                            this.handleCommitMessage(request);
                            break;
                        case ABORT:
                            logger.info("Receive ABORT mssage from client:" + request.getClientName());
                            logger.info("Txn: " + request.getTxnNum());
                            this.handleAbortMessage(request);
                            break;
                        }
                    }
                    else if (wrapper.getmessageclass() == LogPropMessage.class) {
                        LogPropMessage logPropMessage = (LogPropMessage) wrapper.getDeSerializedInnerMessage();
                        logger.info("Datacenter:" + this.dataCenterName + " get log prop from Datacenter:"
                                + logPropMessage.getSourceDataCenterName());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Description: TODO
     * 
     * @param request
     *            void
     */
    private void handleAbortMessage(ClientRequestMessage request) {
        // TODO Auto-generated method stub

    }

    /**
     * Description: TODO
     * 
     * @param request
     *            void
     * @throws IOException
     */
    private void handleCommitMessage(ClientRequestMessage request) throws IOException {
        long txnNum = request.getTxnNum();
        String clientName = request.getRoutingKey();

        PreparingTransaction txn = uncommitedTxnDetails.get(txnNum);

        // If conflict or being overriten just abort
        if (isConflict(txn) || isOverriten(txn)) {
            uncommitedTxnDetails.remove(txnNum);
            CenterResponseMessageFactory.createAbortReponseMessage(clientName, txnNum);
            return;
        }
        txn.setTimestamp(Common.getTimeStamp());
        txn.generateKTS(this.commitOffsets);

        // Add to local preparing txns
        PTPool.add(txn);
        // Add to log
        logs.add(new Log(txn));
        // Move out of uncommitedTxns
        uncommitedTxnDetails.remove(txnNum);
    }

    /**
     * Description: TODO
     * 
     * @param request
     *            void
     * @throws IOException
     */
    private void handleReadMessage(ClientRequestMessage request) throws IOException {
        long txnNum = request.getTxnNum();
        String clientName = request.getRoutingKey();

        // Do the read
        DatastoreEntry entry = datastore.readValue(request.getReadKey());

        // Add key/version to read set
        Transaction t = uncommitedTxnDetails.get(request.getTxnNum());
        t.getReadSet().put(request.getReadKey(), entry.getVersion());

        // Response user with read version and value
        CenterResponseMessage readResponseMessage = CenterResponseMessageFactory.createReadResponseMessage(
                clientName,
                txnNum,
                entry);
        sendCenterResponseMessage(readResponseMessage);

    }

    /**
     * Description: TODO
     * 
     * @param request
     *            void
     */
    private void handleWriteMessage(ClientRequestMessage request) {
        long txnNum = request.getTxnNum();
        String clientName = request.getRoutingKey();

        // Do the write
        datastore.writeValue(request.getWriteKey(), request.getWriteValue());

        Transaction t = uncommitedTxnDetails.get(request.getTxnNum());
        t.getWriteSet().put(request.getWriteKey(), request.getWriteValue());
    }

    /**
     * Description: Add transaction to dataCenter's txnDetail list.
     * Respond client with txnNum.
     * 
     * @param request
     *            void
     * @throws IOException
     * @throws InterruptedException
     */
    private void handleBeginMessage(ClientRequestMessage request) throws IOException, InterruptedException {
        // Create an entry in txnDetails
        long txnNum = generateTxnNum();
        String clientName = request.getRoutingKey();

        // Create a transactionDetail with a preparingTransaction entity inside
        PreparingTransaction txn = new PreparingTransaction(txnNum, clientName, this.dataCenterName);
        uncommitedTxnDetails.put(txnNum, txn);

        CenterResponseMessage beginResponse = CenterResponseMessageFactory.createBeginResponseMessage(
                clientName,
                txnNum
                );

        sendCenterResponseMessage(beginResponse);

    }

    private static class LogPropagationThread implements Runnable {
        private DataCenter fromDataCenter;
        private String toDataCenterName;

        public LogPropagationThread(DataCenter fromDataCenter, String toDataCenterName) {
            super();
            this.fromDataCenter = fromDataCenter;
            this.toDataCenterName = toDataCenterName;
        }

        public void run() {
            // TODO Auto-generated method stub
            while (true) {
                if (fromDataCenter.isAutoLogPropagation) {
                    try {
                        // Sleep for a specific RTT to simulate the latency
                        Thread.sleep(fromDataCenter.RTTLatencies.get(toDataCenterName));
                        // Send log to toDataCenter, message info including sourceDataCenter info and transactions that
                        // need to be propagated
                        fromDataCenter.sendLogPropagationMessage(toDataCenterName,
                                new LogPropMessage(fromDataCenter.dataCenterName,
                                        "Propagation"));
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }

    }

    /**
     * PRIVATE UTIL METHODS
     */

    private long generateTxnNum() {
        long value = this.txnNumGenerator;
        this.txnNumGenerator++;
        return value;
    }

    /**
     * Description: TODO
     * 
     * @param txn
     * @return
     *         boolean
     */
    private boolean isOverriten(PreparingTransaction txn) {
        HashMap<String, Long> readSet = txn.getReadSet();
        Set<String> readKeySet = readSet.keySet();
        Iterator<String> iter = readKeySet.iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            Long txnVersion = readSet.get(key);
            // FIXME Datastore operations
            Long upToDateVersion = datastore.readValue(key).getVersion();
            if (!txnVersion.equals(upToDateVersion)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Description: TODO
     * 
     * @param txn
     * @param iter
     * @return
     *         boolean
     */
    private boolean isConflict(PreparingTransaction txn) {
        boolean isConflict = false;
        Iterator<PreparingTransaction> iter = PTPool.iterator();
        // Local & External preparing transactions conflict detections
        while (iter.hasNext()) {
            if (isTwoTransactionConflict(txn, iter.next()))
            {
                isConflict = true;
                break;
            }
        }
        if (!isConflict) {
            iter = EPTPool.iterator();
            while (iter.hasNext()) {
                if (isTwoTransactionConflict(txn, iter.next()))
                {
                    isConflict = true;
                    break;
                }
            }
        }
        return isConflict;
    }

    /**
     * Description: TODO
     * 
     * @param txn
     * @param next
     * @return
     *         boolean
     */
    private boolean isTwoTransactionConflict(Transaction txn, Transaction localOrExtTxn) {
        Set<String> txnReadSet = txn.getReadSet().keySet();
        Set<String> txnWriteSet = txn.getWriteSet().keySet();

        Set<String> localOrExtTxnWriteSet = localOrExtTxn.getWriteSet().keySet();
        for (String s : localOrExtTxnWriteSet) {
            if (txnReadSet.contains(s) || txnWriteSet.contains(s))
                return false;
        }
        return true;
    }

    /**
     * 
     * Description: Log scan
     * @throws IOException
     * void
     */
    public void processLogs() throws IOException {
        Iterator<Log> iter = logs.iterator();
        while (iter.hasNext()) {
            Log l = iter.next();
            Transaction t = l.getTransaction();
            // Skip local transactions
            if (t.getDatacenterName().equals(this.dataCenterName)) {
                continue;
            }
            // Check conflict with local Preparing Transaction
            Iterator<PreparingTransaction> ptPoolIter = PTPool.iterator();
            while (ptPoolIter.hasNext()) {
                PreparingTransaction ptPoolTxn = ptPoolIter.next();
                // FIXME
                // Log contains a conflicting transaction with local preparing transaction, Abort local preparing
                // transaction
                if (isTwoTransactionConflict(t, ptPoolTxn)) {
                    // Create an aborted log
                    FinishedTransaction finishedTranscation = new FinishedTransaction(ptPoolTxn);
                    finishedTranscation.setCommitted(false);
                    finishedTranscation.setTimestamp(Common.getTimeStamp());
                    // Sent abort message
                    sendCenterResponseMessage(CenterResponseMessageFactory.createAbortReponseMessage(
                            finishedTranscation.getClientName(), finishedTranscation.getTxnNum()));
                    logs.add(new Log(finishedTranscation));
                }

                // Log contains a preparing transaction
                if (l.isPreparing()) {
                    EPTPool.add((PreparingTransaction) l.getTransaction());
                }
                // FIXME Database operation
                // Log contains a finished transaction
                else if (l.isFinished()) {
                    // Commited one, don't care aborted one
                    if (((FinishedTransaction) t).isCommitted()) {
                        HashMap<String, String> writeSet = t.getWriteSet();
                        Iterator<String> writeSetIter = writeSet.keySet().iterator();
                        while (iter.hasNext()) {
                            String key = writeSetIter.next();
                            datastore.writeValue(key, writeSet.get(key));
                        }
                    }

                    // Remove this transaction from EPTPool
                    PreparingTransaction target = null;
                    Iterator<PreparingTransaction> iterator = EPTPool.iterator();
                    while (iterator.hasNext()) {
                        PreparingTransaction next = iterator.next();
                        if (next.getTxnNum() == t.getTxnNum()) {
                            target = next;
                            break;
                        }
                    }
                    if (target == null) {
                        logger.info("Error while removing preparing transaction from EPTPool");
                    }
                    EPTPool.remove(target);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ArrayList<DataCenter> dataCenterList = new ArrayList<DataCenter>();
        String[] dataCenterNames = { "dc1", "dc2" };
        int[] dataCenterLocations = { 10000, 15000 };
        for (int i = 0; i < dataCenterNames.length; i++) {
            dataCenterList.add(new DataCenter(dataCenterNames[i], 0, dataCenterLocations[i], dataCenterNames,
                    dataCenterLocations));
        }
        for (int i = 0; i < dataCenterNames.length; i++) {
            dataCenterList.get(i).generateRTTList();
            dataCenterList.get(i).generateCommitOffsets();
        }
        for (int i = 0; i < dataCenterList.size(); i++) {
            new Thread(dataCenterList.get(i)).start();
        }
    }
}
