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
import helios.transaction.Transaction;
import helios.transaction.TransactionFactory;
import helios.transaction.TransactionType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

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
    // private long[] rDict;
    private HashMap<String, Long> rDict;

    // To simulate log propagation latencies, generate by subtracting location attribute
    private HashMap<String, Integer> RTTLatencies;

    // Each center is assigned a commitOffset to other data center, used to decide whether to commit or abort, now just
    // set to 0
    private HashMap<String, Integer> commitOffsets;

    // Local preparing transaction pool
    private TreeSet<Transaction> PTPool;

    // External transaction pool
    private TreeSet<Transaction> EPTPool;

    // Shared log pool
    private TreeSet<Log> logs;

    // For the sake of simplicity, create an unsent log pool for log propagation
    private TreeSet<Log> unsentLogs;

    // To keep user's transaction that haven't been committed by user. TxnNum -> TransactionDetail
    // Will generate a transaction in PTPool and a log in logs when user commit it
    private HashMap<Long, Transaction> uncommitedTxnDetails;

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

    public DataCenter(String dataCenterName, int location, String[] dataCenterNames,
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
        rDict = new HashMap<String, Long>();
        PTPool = new TreeSet<Transaction>();
        EPTPool = new TreeSet<Transaction>();
        uncommitedTxnDetails = new HashMap<Long, Transaction>();

        logs = new TreeSet<Log>();
        unsentLogs = new TreeSet<Log>();
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
                        logger.info(this.dataCenterName + " receive " + t + " message from client "
                                + request.getClientName());
                        logger.info(request);
                        switch (t) {
                        case BEGIN:
                            this.handleBeginMessage(request);
                            break;
                        case WRITE:
                            this.handleWriteMessage(request);
                            break;
                        case READ:
                            this.handleReadMessage(request);
                            break;
                        case COMMIT:
                            this.handleCommitMessage(request);
                            break;
                        case ABORT:
                            this.handleAbortMessage(request);
                            break;
                        }
                    }
                    else if (wrapper.getmessageclass() == LogPropMessage.class) {
                        LogPropMessage logPropMessage = (LogPropMessage) wrapper.getDeSerializedInnerMessage();
                        // logger.info("Datacenter:" + this.dataCenterName + " get log prop from Datacenter:"
                        // + logPropMessage.getSourceDataCenterName());
                        Log[] deliveredLogs = logPropMessage.getLogs();
                        for (Log l : deliveredLogs) {
                            this.logs.add(l);
                            logger.info(l);
                        }
                        // Update rDict
                        this.rDict.put(logPropMessage.getSourceDataCenterName(), logPropMessage.getLogPropTimestamp());

                        // logger.info(dataCenterName + " do pt pool scan");
                        this.PTPoolScan();
                        if (deliveredLogs.length > 0) {
                            logger.info(dataCenterName + " do log scan, get " + deliveredLogs.length + " logs");
                            this.logsScan();
                        }
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
        long txnNum = request.getTxnNum();
        String clientName = request.getClientName();

        uncommitedTxnDetails.remove(txnNum);
        CenterResponseMessageFactory.createAbortReponseMessage(clientName, txnNum);
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
        String clientName = request.getClientName();

        Transaction txn = uncommitedTxnDetails.get(txnNum);

        // If conflict or being overriten just abort
        if (isConflict(txn) || isOverriten(txn)) {
            uncommitedTxnDetails.remove(txnNum);
            sendCenterResponseMessage(CenterResponseMessageFactory.createAbortReponseMessage(clientName, txnNum));
            return;
        }
        txn.setTimestamp(Common.getTimeStamp());
        txn.generateKTS(this.commitOffsets);

        // Add to local preparing txns
        PTPool.add(txn);
        // Add to log
        Log l = new Log(txn);
        logs.add(l);
        unsentLogs.add(l);
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
        String clientName = request.getClientName();

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
        String clientName = request.getClientName();

        // Do the write
        datastore.writeValue(request.getWriteKey(), request.getWriteValue());

        Transaction t = uncommitedTxnDetails.get(request.getTxnNum());
        System.out.println(t);
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
        String clientName = request.getClientName();

        // Create a preparing transcation
        Transaction txn = TransactionFactory.createPreparingTransaction(txnNum, clientName,
                this.dataCenterName);
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
                        int logSize = fromDataCenter.unsentLogs.size();
                        Log[] logToBeSent = new Log[logSize];
                        fromDataCenter.unsentLogs.toArray(logToBeSent);
                        fromDataCenter.sendLogPropagationMessage(toDataCenterName,
                                new LogPropMessage(fromDataCenter.dataCenterName, logToBeSent, Common.getTimeStamp()
                                ));
                        fromDataCenter.unsentLogs.clear();

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
    private boolean isOverriten(Transaction txn) {
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
    private boolean isConflict(Transaction txn) {
        boolean isConflict = false;
        Iterator<Transaction> iter = PTPool.iterator();
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
                return true;
        }
        return false;
    }

    /**
     * 
     * Description: Log scan(Algo2)
     * 
     * @throws IOException
     *             void
     */
    public void logsScan() throws IOException {
        Iterator<Log> iter = logs.iterator();
        while (iter.hasNext()) {
            Log l = iter.next();
            logger.info(l);
            // If log is processed, don't process it again
            if (l.isProcessed())
                continue;

            Transaction t = l.getTransaction();
            // Skip local transactions
            if (t.getDatacenterName().equals(this.dataCenterName)) {
                l.setProcessed(true);
                continue;
            }

            // Check conflict with local Preparing Transaction
            Iterator<Transaction> ptPoolIter = PTPool.iterator();
            while (ptPoolIter.hasNext()) {
                Transaction ptPoolTxn = ptPoolIter.next();
                // FIXME
                // Log contains a conflicting transaction with local preparing transaction, Abort local preparing
                // transaction
                if (isTwoTransactionConflict(t, ptPoolTxn)) {
                    // Create an aborted log
                    Transaction finishedTranscation = TransactionFactory
                            .createFinishedTransactionFromPreparingTransaction(ptPoolTxn, false);
                    finishedTranscation.setTimestamp(Common.getTimeStamp());
                    // Sent abort message
                    sendCenterResponseMessage(CenterResponseMessageFactory.createAbortReponseMessage(
                            finishedTranscation.getClientName(), finishedTranscation.getTxnNum()));

                    Log log = new Log(finishedTranscation);
                    logs.add(log);
                    unsentLogs.add(log);
                }
            }

            // Log contains a preparing transaction
            if (l.getTransaction().getType().equals(TransactionType.PREPARING)) {
                EPTPool.add(l.getTransaction());
            }
            // FIXME Database operation
            // Log contains a finished transaction
            else {
                // Commited one, don't care aborted one
                if (t.isCommitted()) {
                    HashMap<String, String> writeSet = t.getWriteSet();
                    Iterator<String> writeSetIter = writeSet.keySet().iterator();
                    while (writeSetIter.hasNext()) {
                        String key = writeSetIter.next();
                        datastore.writeValue(key, writeSet.get(key));
                    }
                    logger.info(this.dataCenterName + " replicate the writeset " + writeSet);
                }

                // Remove this transaction from EPTPool whether it's committed or not
                Transaction target = null;
                Iterator<Transaction> iterator = EPTPool.iterator();
                while (iterator.hasNext()) {
                    Transaction next = iterator.next();
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

            // Set the log as processed
            l.setProcessed(true);
            // FIXME
            // // Update rDict
            // rDict.put(t.getDatacenterName(), t.getTimestamp());
        }
    }

    /**
     * 
     * Description: PTPool scan for local commit(Algo3)
     * void
     * 
     * @throws IOException
     */
    public void PTPoolScan() throws IOException {
        List<Transaction> localTransactionsToBeRemoved = new ArrayList<Transaction>();
        Iterator<Transaction> iterator = PTPool.iterator();

        while (iterator.hasNext()) {
            Transaction pt = iterator.next();
            boolean isCommitable = true;
            for (String otherDatacenter : dataCenterNames) {
                if (this.dataCenterName.equals(otherDatacenter))
                    continue;
                if (rDict.get(otherDatacenter) == null
                        || rDict.get(otherDatacenter) < pt.getKts().get(otherDatacenter)) {
                    isCommitable = false;
                    break;
                }
            }
            if (!isCommitable) {
                continue;
            }

            logger.info(pt.getDatacenterName() + " commit local transaction: " + pt.getTxnNum());
            // Do write operations to datastore
            HashMap<String, String> writeSet = pt.getWriteSet();
            Iterator<String> writeSetIterator = writeSet.keySet().iterator();
            while (writeSetIterator.hasNext()) {
                String key = writeSetIterator.next();
                // FIXME datastore operations
                datastore.writeValue(key, writeSet.get(key));
            }

            // Create a finished log
            Transaction finishedTransaction = TransactionFactory.createFinishedTransactionFromPreparingTransaction(
                    pt, true);
            finishedTransaction.setTimestamp(Common.getTimeStamp());
            finishedTransaction.setCommitted(true);
            Log l = new Log(finishedTransaction);
            logs.add(l);
            unsentLogs.add(l);

            // Remove this preparing transaction from PTPool
            localTransactionsToBeRemoved.add(pt);

            // Send commit message to client
            sendCenterResponseMessage(CenterResponseMessageFactory.createCommitResponseMessage(pt.getClientName(),
                    pt.getTxnNum()));
        }
        // Remove from PTPool
        PTPool.removeAll(localTransactionsToBeRemoved);
    }

    public static void main(String[] args) throws Exception {
        ArrayList<DataCenter> dataCenterList = new ArrayList<DataCenter>();
        String[] dataCenterNames = { "dc1", "dc2" };
        int[] dataCenterLocations = { 10000, 15000 };
        for (int i = 0; i < dataCenterNames.length; i++) {
            dataCenterList.add(new DataCenter(dataCenterNames[i], dataCenterLocations[i], dataCenterNames,
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
