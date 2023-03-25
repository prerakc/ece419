package app_kvServer;

import client.KVStore;
import logger.LogSetup;
import shared.Config;
import shared.messages.IKVMessage.StatusType;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvECS.ECSClient;
import storage.KVStorage;
import storage.HashRing;
import storage.HashUtils;
import ecs.ECSNode;
import shared.messages.IKVMessage.StatusType;
import runnables.ShutdownRunnable;

import java.net.*;
import java.lang.Integer;
import java.io.IOException;
import java.util.*;
import java.io.*;


public class KVServer extends Thread implements IKVServer {
	private static Logger logger = Logger.getRootLogger();

	private ServerSocket serverSocket;
	private int port;
	private int cacheSize;
	private String strategy;
	private boolean running;
	private static String serverName;
	private static StatusType status;
	private String address;
	private String dataDirectory;
	private String dataProperties;

	private KVStorage storage;

	private ArrayList<Thread> threads;
	
	private static HashRing metaData;

	private ECSNode serverNode;
	private static ECSClient ecsClient;
	private String ecsServer;
	private int ecsPort;

	public static int serverStatus;

	private boolean distributedMode;

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	 //TODO: add support for serverName
	public KVServer(String address, int port, int cacheSize, String strategy, String ecsServer, int ecsPort) {
		// TODO Auto-generated method stub
		this(address, port, cacheSize, strategy, ecsServer, ecsPort, "./data", String.format("%s:%d.properties", address, port));
	}

	public KVServer(String address, int port, int cacheSize, String strategy, String ecsServer, int ecsPort, String dataDir, String dataProps) {
		// TODO Auto-generated method stub
		this.address = address;
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.serverName = String.format("%s:%d",this.address,port);
		KVServer.serverName = String.format("%s:%d",this.address,port);
		// this.dataProperties = KVServer.serverName + ".properties";
		this.dataProperties = dataProps;
		this.dataDirectory = dataDir;
		KVServer.status = StatusType.SERVER_NOT_AVAILABLE;

		// this.serverName = String.format("%s:%d",this.getHostname(),port);

		this.storage = new KVStorage(this.dataDirectory, this.dataProperties);

		this.threads = new ArrayList<Thread>();

		KVServer.metaData = new HashRing();

		// start up ecs node
		this.serverNode = new ECSNode(this.serverName,this.address, this.port);
		this.serverNode.setStatus(StatusType.SERVER_STOPPED);

		KVServer.ecsClient = new ECSClient(ecsServer,ecsPort,false);
		KVServer.ecsClient.addKVServer(KVServer.serverName);

		KVServer.ecsClient.addStatusServerWatch(KVServer.serverName);
		KVServer.ecsClient.addMetadataServerWatch(KVServer.serverName);

		distributedMode = true;
	}

	public KVServer(String address, int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this(address, port, cacheSize, strategy, "./data", String.format("%s_%d.properties", address, port));
	}

	public KVServer(String address, int port, int cacheSize, String strategy, String dataDir, String dataProps) {
		// TODO Auto-generated method stub
		this.address = address;
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;

		this.status = StatusType.SERVER_IDLE;

		this.dataDirectory = dataDir;
		this.dataProperties = dataProps;

		this.storage = new KVStorage(this.dataDirectory, this.dataProperties);

		this.threads = new ArrayList<Thread>();

		distributedMode = false;
	}
	
	public StatusType getStatus(){
		return KVServer.status;
	}

	public static void updateStatusZK(String status){
		// logger.info("++++++++++IN KVSERVER UPDATE STATUS+++++++++++");
		try{
			KVServer.status = StatusType.values()[Integer.parseInt(status)];
		} catch (NumberFormatException e){
			// KVServer.server = StatusType.valueOf(status);
		}
	}

	public static Properties getAllKeysInRange(ECSNode node, String[] keyRange){
		Properties dataProps = new Properties();
		String dataPath = String.format("data/%s.properties", node.getNodeName());
		Properties toBeMoved = new Properties();
		//load data
		try {
			dataProps.load(new FileInputStream(dataPath));
		} catch (Exception e) {
			logger.error("Failed to write database to disk", e);
		}

		//add all keys in range
		for (String key: dataProps.stringPropertyNames()) {
				logger.info("Checking key key: " + key);
				if(HashRing.keyInRange(key, keyRange)){
					logger.info("Transferring key: " + key);
                	toBeMoved.put(key, dataProps.getProperty(key));
				}
        }

		return toBeMoved;

	}

	public static void transferData(ECSNode srcNode, ECSNode dstNode){
		transferData(srcNode, dstNode, null, true);
	}

	public static void transferData(ECSNode srcNode, ECSNode dstNode, String[] keyRange){
		transferData(srcNode, dstNode, keyRange, true);
	}	

	public static void transferData(ECSNode srcNode, ECSNode dstNode, boolean deleteData){
		transferData(srcNode, dstNode, null, deleteData);
	}

	public static void transferData(ECSNode srcNode, ECSNode dstNode, String[] keyRange, boolean deleteData){
		logger.info("I am here transfering node data from " + srcNode.getNodeName() + "to node " + dstNode.getNodeName());
		logger.info("Transfering all data in key range " + Arrays.toString(keyRange));
		keyRange = keyRange == null ? dstNode.getNodeHashRange() : keyRange;
		Properties toBeMoved = getAllKeysInRange(srcNode, keyRange);

		try {
			KVStore client = new KVStore(dstNode.getNodeHost(), dstNode.getNodePort());
			client.connect();
			for (String key: toBeMoved.stringPropertyNames()) {
					client.datatransfer(key, toBeMoved.getProperty(key));
        	}

			client.disconnect();

			//
		} catch (Exception e) {
			logger.error("Failed to transfer data", e);
		}	

		// try {
		// 	Thread.sleep(5000);
		// } catch (InterruptedException e) {
		// 	Thread.currentThread().interrupt();
		// }
		  
		if(!deleteData) return;
		//delete keys from file 
		Properties newSrcProperties = new Properties();
		String srcDataPath = String.format("data/%s.properties", srcNode.getNodeName());
		try {
				newSrcProperties.load(new FileInputStream(srcDataPath));
				for (String key: toBeMoved.stringPropertyNames()) {
					newSrcProperties.remove(key);
        		}
				newSrcProperties.store(new FileOutputStream(srcDataPath), null);
			} catch (Exception e) {
				logger.error("Failed to write database to disk", e);
		}

	}

	public static void transferAllDataToSuccessor(ECSNode node){
		ECSNode successor = metaData.getSuccessorNodeFromIpHash(node.getIpPortHash());
		String[] nodeHashRange = KVServer.metaData.getServerForHashValue(serverName).getNodeHashRange();
		String[] loopedHashRange = new String[] {HashRing.incrementHexString(nodeHashRange[1]), nodeHashRange[1]};
		
		
		// successor = successor == null ? metaData.getFirstValue() : successor; //metadata might be getting updated before the shutdown occurs? 
		if(successor == null) return; // There are no more nodes in the ring

		// transferData(node, successor, nodeHashRange);
		transferData(node, successor, loopedHashRange);
	}

	public static void handleReplicaDataOnShutdown(ECSNode node){
		ECSNode updatedNode = KVServer.metaData.getServerForHashValue(KVServer.serverName);
		HashRing metaData = KVServer.metaData;
		int numNodesAfterRemoval = metaData.getHashRing().size() - 1;
		if(numNodesAfterRemoval < 3) return;

		ECSNode successorNode = metaData.getSuccessorNodeFromIpHash(node.getIpPortHash());
		ECSNode predNode = metaData.getPredecessorNodeFromIpHash(node.getIpPortHash());
		ECSNode secondSuccessorNode = metaData.getSuccessorNodeFromIpHash(successorNode.getIpPortHash());
		ECSNode secondPredNode =  metaData.getPredecessorNodeFromIpHash(predNode.getIpPortHash());
		ECSNode thirdSuccessorNode = metaData.getSuccessorNodeFromIpHash(secondSuccessorNode.getIpPortHash());

		transferData(predNode, secondSuccessorNode, predNode.getNodeHashRange(),false);
		transferData(updatedNode, thirdSuccessorNode, updatedNode.getNodeHashRange(), false);
	}

	public static void handleShutdown(ECSNode node){
		// logger.info("The hash range of the server being shut down: " + Arrays.toString(KVServer.metaData.getServerForHashValue(serverName).getNodeHashRange()));
		handleReplicaDataOnShutdown(node);
		KVServer.transferAllDataToSuccessor(node);
		KVServer.ecsClient.removeKVServer(KVServer.serverName);
		KVServer.ecsClient.removeServerStatus(KVServer.serverName);
		// KVServer.transferAllDataToSuccessor(node);
	}

	public static void updateMetadataZK(String status){
		logger.info(status);
		HashRing newMeta = HashRing.getHashRingFromNodeMap(ECSNode.deserializeToECSNodeMap(status));
		HashRing oldMeta = KVServer.metaData;
		KVServer.metaData = newMeta;
		logger.info( KVServer.metaData.getHashRing().firstEntry().getValue().getNodeHashRange()[0] + ":" + KVServer.metaData.getHashRing().firstEntry().getValue().getNodeHashRange()[1]);

		ECSNode thisNode = getCurrentServerNode(oldMeta, newMeta);
		String thisNodeHash = thisNode.getIpPortHash();

		if(currentServerIsAdded(oldMeta, newMeta)){
			logger.info(String.format("%s was added and is obtaining keys from successor", thisNode.getNodeName()));
			ECSNode successorNode = newMeta.getSuccessorNodeFromIpHash(thisNode.getIpPortHash());
			if(successorNode != null)
				transferData(successorNode, thisNode, false);

			ECSNode predNode = newMeta.getPredecessorNodeFromIpHash(thisNode.getIpPortHash());
			int numNodes = newMeta.getHashRing().size();
			if(numNodes <= 1) return; // numNodes shoudl never be 0 but whatev
			else if(numNodes == 2){
				transferData(predNode, thisNode, predNode.getNodeHashRange(), false);
			}else if(numNodes == 3){
				transferData(predNode, thisNode, predNode.getNodeHashRange(), false);
				transferData(successorNode, thisNode, successorNode.getNodeHashRange(), false);
			}else{
				ECSNode secondSuccessorNode = newMeta.getSuccessorNodeFromIpHash(successorNode.getIpPortHash());
				transferData(secondSuccessorNode, thisNode, predNode.getNodeHashRange(),true);

				ECSNode secondPredNode = newMeta.getPredecessorNodeFromIpHash(predNode.getIpPortHash());
				transferData(successorNode, thisNode, secondPredNode.getNodeHashRange(), true);

				//have to remove all the keys in thisNode's hashrange from third successor
				ECSNode thirdSuccessorNode = newMeta.getSuccessorNodeFromIpHash(secondSuccessorNode.getIpPortHash());
				transferData(thirdSuccessorNode, thisNode, thisNode.getNodeHashRange(),true);
			}
			// if(predNode != null){
			// 	transferData(predNode, thisNode, predNode.getNodeHashRange(), false);
			// }

			// Set<ECSNode> nearbyNodes = new HashSet<ECSNode>();
			// nearbyNodes.addAll(Arrays.asList( new ECSNode[] {predNode, successorNode, thisNode} ));

			// if(successorNode != null){
			// 	ECSNode secondSuccessorNode = newMeta.getSuccessorNodeFromIpHash(successorNode.getIpPortHash());
			// 	if(secondSuccessorNode != null && predNode != null && !nearbyNodes.contains(secondSuccessorNode))
			// 		transferData(secondSuccessorNode, thisNode, predNode.getNodeHashRange(),true);
			// }
			
			// if(predNode != null){
			// 	ECSNode secondPredNode = newMeta.getPredecessorNodeFromIpHash(predNode.getIpPortHash());
			// 	if(secondPredNode != null && successorNode != null && !nearbyNodes.contains(secondPredNode))
			// 		transferData(successorNode, thisNode, secondPredNode.getNodeHashRange(), true);
			// }
		}
	}

		
		// logger.info("ADDED IN UPDATE "+ECSNode.deserializeToECSNodeMap(status).keySet());

	//iterate through new metadata to see if the server's node is missing
	public static ECSNode getCurrentServerNode(HashRing oldMeta, HashRing newMeta){
		for ( Map.Entry<String, ECSNode> entry : newMeta.getHashRing().entrySet()) {
			if(entry.getValue().getNodeName().equals(KVServer.serverName)){
				return entry.getValue();
			}
		}

		for ( Map.Entry<String, ECSNode> entry : oldMeta.getHashRing().entrySet()) {
			if(entry.getValue().getNodeName().equals(KVServer.serverName)){
				return entry.getValue();
			}
		}

		return null;
	}
	public static boolean wasNodeRemoved(HashRing oldMeta, HashRing newMeta, String ipPortHash){
		logger.info("Checking if server removed");
		ECSNode responsibleNode = oldMeta.getServerForHashValue(ipPortHash);
		return (!responsibleNode.getIpPortHash().equals(ipPortHash));
	}

	public static ECSNode getRemovedNode(HashRing oldMeta, HashRing newMeta, String ipPortHash){
		logger.info("Checking if server removed");
		logger.info(ipPortHash);
		for ( Map.Entry<String, ECSNode> entry : oldMeta.getHashRing().entrySet()) {
			String entryHash = entry.getValue().getIpPortHash();
			ECSNode responsibleNode = newMeta.getServerForHashValue(entryHash);
			logger.info("rehash " + responsibleNode.getIpPortHash());
			logger.info("entryHash " + entryHash);
			if(!responsibleNode.getIpPortHash().equals(entryHash))
				return entry.getValue();

		}
		return null;
	}

	public static boolean currentServerIsRemoved(HashRing oldMeta, HashRing newMeta){
		logger.info("Checking is server removed");
		for ( Map.Entry<String, ECSNode> entry : newMeta.getHashRing().entrySet()) {
			if(entry.getValue().getNodeName().equals(KVServer.serverName)){
				return false;
			}
		}
		return true;
	}

	public static boolean currentServerIsAdded(HashRing oldMeta, HashRing newMeta){
		for ( Map.Entry<String, ECSNode> entry : oldMeta.getHashRing().entrySet()) {
			if(entry.getValue().getNodeName().equals(KVServer.serverName)){
				return false;
			}
		}
		return true;
	}


	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		return port;
	}

	@Override
	public String getHostname(){
		String hostname = "";
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			logger.error("The server hostname cannot be resolved. \n", e);
		}
		return hostname;
	}


	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		return IKVServer.CacheStrategy.None;
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return cacheSize;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return storage.exists(key);
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		if(!inStorage(key)) {
			throw new Exception(String.format("Key '%s' is not in the database", key));
		}

		String value = storage.get(key);

		if(value == null) {
			throw new Exception(String.format("Failed to get key '%s' from the database", key));
		} else {
			return value;
		}
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
		if  (value.equals("null") || value.isEmpty()) {
			throw new Exception("Cannot put a key with null value into the database");
		}

		if (!storage.put(key, value)) {
			throw new Exception(String.format("Failed to put key '%s' and value '%s' into the database", key, value));
		}
	}

	public void putKVReplica(String key, String value) throws Exception{
		this.putKV(key,value);
		ECSNode thisNode = KVServer.metaData.getServerForHashValue(this.serverNode.getIpPortHash());
		String thisNodeHash = thisNode.getIpPortHash();

		logger.info(String.format("%s was added and is obtaining keys from successor", thisNode.getNodeName()));
		// get first succesor node
		ECSNode successorNodeFirst = KVServer.metaData.getSuccessorNodeFromIpHash(thisNode.getIpPortHash());
		if(successorNodeFirst != null){
			// get second if first is found
			ECSNode successorNodeSecond = KVServer.metaData.getSuccessorNodeFromIpHash(successorNodeFirst.getIpPortHash());
			if(successorNodeSecond != null)
				putReplica(successorNodeSecond,key,value);
			putReplica(successorNodeFirst,key,value);
		}
	}

	private void putReplica(ECSNode dstNode, String key, String value){
		try {
			KVStore client = new KVStore(dstNode.getNodeHost(), dstNode.getNodePort());
			client.connect();
			client.datatransfer(key, value);
			client.disconnect();
		} catch (Exception e) {
			logger.error("Failed to replicate data", e);
		}
	}

	private void deleteReplica(ECSNode dstNode, String key){
		try {
			KVStore client = new KVStore(dstNode.getNodeHost(), dstNode.getNodePort());
			client.connect();
			client.forceDelete(key,"");
			client.disconnect();
		} catch (Exception e) {
			logger.error("Failed to replicate data", e);
		}
	}

	public void deleteKVReplica(String key) throws Exception{
		this.deleteKV(key);
		ECSNode thisNode = KVServer.metaData.getServerForHashValue(this.serverNode.getIpPortHash());
		String thisNodeHash = thisNode.getIpPortHash();

		logger.info(String.format("%s was removed and is obtaining keys from successor", thisNode.getNodeName()));
		// get first succesor node
		ECSNode successorNodeFirst = KVServer.metaData.getSuccessorNodeFromIpHash(thisNode.getIpPortHash());
		logger.info("FIRST SUCC: " + successorNodeFirst.getIpPortHash());
		if(successorNodeFirst != null){
			// get second if first is found
			ECSNode successorNodeSecond = KVServer.metaData.getSuccessorNodeFromIpHash(successorNodeFirst.getIpPortHash());
			if(successorNodeSecond != null)
				deleteReplica(successorNodeSecond,key);
			
			logger.info("SEC SUCC: " + successorNodeSecond.getIpPortHash());
			
			deleteReplica(successorNodeFirst,key);
		}
	}

	public void deleteKV(String key) throws Exception{
		if(!inStorage(key)) {
			throw new Exception(String.format("Key '%s' is not in the database", key));
		}

		if(!storage.delete(key)) {
			throw new Exception(String.format("Failed to delete key '%s' from the database", key));
		}
	}

	public void moveData() {
		// assume metadata has been updated before the function is called (i.e. metadata no longer contains this server)

		// this is the last server
		if (metaData.getHashRing().isEmpty()) {
			return;
		}

		try {
			Map<String, String> db = storage.getDatabase();

			String randomKey = db.entrySet().iterator().next().getKey();

			ECSNode successor = metaData.getServerForKVKey(randomKey);

			KVStore client = new KVStore(successor.getNodeHost(), successor.getNodePort());

			client.connect();

			for (Map.Entry<String, String> kvPair : db.entrySet()) {
				client.put(kvPair.getKey(), kvPair.getValue());
			}

			client.disconnect();

			storage.clear();
		} catch (Exception ignored) { // assuming KVStore connect() and put() don't throw errors
		}
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
		storage.clear();
	}

	@Override
    public void run(){
		// TODO Auto-generated method stub
		if (distributedMode) {
			ShutdownRunnable shutdownRunnable = new ShutdownRunnable(this.serverNode);
			Runtime.getRuntime().addShutdownHook(new Thread(shutdownRunnable));
		}

		running = initializeServer();

		if(serverSocket != null) {
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();

					TempClientConnection connection = new TempClientConnection(client, this);

					Thread thread = new Thread(connection);

					thread.start();

					threads.add(thread);

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
		
	}

	private boolean isRunning() {
		return this.running;
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
		running = false;
		try {
			for (Thread thread: threads) {
				thread.interrupt();
			}
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	public boolean isResponsibleForRequest(String key){
		if (distributedMode) {
		  logger.info("This server's IP:Port Hash" + this.serverNode.getIpPortHash());
		  logger.info(KVServer.metaData.getHashRing().keySet());
		  // sometimes returns a nullpointer exception, probably get returns null
		  ECSNode node = KVServer.metaData.getHashRing().get(this.serverNode.getIpPortHash());
		  if(node == null){
			logger.warn("Server could not get it's own node! There is likely a timing bug");
		  }
		  String[] hashRange = node.getNodeHashRange();
		  // String[] hashRange = this.serverNode.getNodeHashRange();
		  key = HashUtils.getHashString(key);
		  logger.info("THE KEY IS: " + key);
		  logger.info("THE RANGE LOWER IS: " + hashRange[0]);
		  logger.info("THE RANGE UPPER IS: " + hashRange[1]);

		  if(hashRange[0].compareTo(hashRange[1]) < 0){//if hash range does not wrap around
			logger.info("key.compareTo(hashRange[0]) > 0: " + (key.compareTo(hashRange[0]) > 0));
			logger.info("key.compareTo(hashRange[1] " + (key.compareTo(hashRange[1]) <= 0));
			return (key.compareTo(hashRange[0]) > 0) &&  (key.compareTo(hashRange[1]) <= 0);
		  }
		  logger.info("key.compareTo(hashRange[0]) < 0: " + (key.compareTo(hashRange[0]) < 0));
		  logger.info("key.compareTo(hashRange[1]) > 0 " + (key.compareTo(hashRange[1]) > 0));
		  return (key.compareTo(hashRange[0]) > 0) ||  (key.compareTo(hashRange[1]) <= 0); //hash range does wrap around
		} else {
			return true;
		}
	}

	public boolean isResponsibleForGet(String key){
		if (distributedMode) {
		  	String keyHash = HashUtils.getHashString(key);
			ECSNode coordinator = KVServer.metaData.getServerForHashValue(keyHash);

			boolean isPrimary = coordinator.getIpPortHash().equals(this.serverNode.getIpPortHash());

			boolean isReplica = KVServer.metaData.isReplicaOfNode(coordinator, this.serverNode);

			return (isPrimary || isReplica);

		} else {
			return true;
		}
	}

	public String serializeHashRing() throws Exception{
		return this.serverNode.serialize();
	}

	public String getMetaDataKeyRanges(){
		return this.metaData.getSerializedHashRanges();
	}

	public String getMetaDataKeyRangesWithRep(){
		return this.metaData.getSerializedHashRangesWithRep();
	}

	public String serializeMetaData(){
		try{
			return ECSNode.serializeECSMap(this.metaData.getHashRing());
		} catch (Exception e){
			logger.error("Error serializing meta data", e);
		}
		return null;
	}
	

	public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.INFO);
			if(args.length != 6) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <address> <port> <cachesize> <cachetype> <ECS_server> <ECS_port>!");
			} else {
				String addr = args[0];
				int port = Integer.parseInt(args[1]);
				int cacheSize = Integer.parseInt(args[2]);
				String strategy = args[3];
				String ecsServer = args[4];
				int ecsPort = Integer.parseInt(args[5]);
				new KVServer(addr,port, cacheSize, strategy, ecsServer,ecsPort).start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <address> <port> <cachesize> <cachetype> <ECS_server> <ECS_port>!");
			System.exit(1);
		}
	}
}