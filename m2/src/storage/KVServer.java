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

	public static void updateMetadataZK(String status){
		logger.info(status);
		HashRing newMeta = HashRing.getHashRingFromNodeMap(ECSNode.deserializeToECSNodeMap(status));
		HashRing oldMeta = KVServer.metaData;
		KVServer.metaData = newMeta;
		logger.info( KVServer.metaData.getHashRing().firstEntry().getValue().getNodeHashRange()[0] + ":" + KVServer.metaData.getHashRing().firstEntry().getValue().getNodeHashRange()[1]);

		// ECSNode thisNode = currentServerIsRemoved(oldMeta, newMeta);
		ECSNode thisNode = getCurrentServerNode(oldMeta, newMeta);
		String thisNodeHash = thisNode.getIpPortHash();
		logger.info("*******************Is the node removed******************");
		// if(thisNode != null){
		ECSNode removedNode = getRemovedNode(oldMeta, newMeta, thisNodeHash);
		// if(currentServerIsRemoved(oldMeta, newMeta)){
		if(removedNode != null){
			logger.info("I am here transfering removed node data from " + removedNode.getNodeName());
			Map<String, String> toBeMoved = Collections.synchronizedMap(new HashMap<String, String>());
			// ECSNode targetNode = oldMeta.getSuccessorNodeFromIpHash(thisNode.getIpPortHash());
			ECSNode targetNode = thisNode;
			if(targetNode == null) return;

			Properties dataProps = new Properties();

			Properties targetDataProps = new Properties();
			String targetDataPath = String.format("data/%s.properties", targetNode.getNodeName());
			String srcPath = String.format("data/%s.properties", removedNode.getNodeName());
			try {
				dataProps.load(new FileInputStream(srcPath));
				targetDataProps.load(new FileInputStream(targetDataPath));
				targetDataProps.putAll(dataProps);
				targetDataProps.store(new FileOutputStream(targetDataPath), null);
			} catch (Exception e) {
				logger.error("Failed to write database to disk", e);
			}			
		}

		
		// thisNode = currentServerIsAdded(oldMeta, newMeta);
		// if(thisNode != null){
		if(currentServerIsAdded(oldMeta, newMeta)){
			ECSNode targetNode = thisNode;
			logger.info("here transferring data to added node: " +  targetNode.getNodeName());
			Map<String, String> toBeMoved = Collections.synchronizedMap(new HashMap<String, String>());
			// ECSNode targetNode = newMeta.getPredecessorNodeFromIpHash(thisNode.getIpPortHash());
			ECSNode srcNode = newMeta.getSuccessorNodeFromIpHash(thisNode.getIpPortHash());
			if(srcNode == null) return;

			Properties dataProps = new Properties();
			String srcDataPath = String.format("data/%s.properties", srcNode.getNodeName());
            try{
				dataProps.load(new FileInputStream(srcDataPath));
			}catch (Exception e) {
				logger.error("Failed to write database to disk", e);
			}

			Properties newCurrentDataProps = new Properties();
			for (String key: dataProps.stringPropertyNames()) {
				logger.info("Checking key key: " + key);
				if(HashRing.keyInRange(key, targetNode.getNodeHashRange())){
					logger.info("Transferring key: " + key);
                	newCurrentDataProps.put(key, dataProps.getProperty(key));
				}
            }


			Properties targetDataProps = new Properties();
			String targetDataPath = String.format("data/%s.properties", targetNode.getNodeName());
			try {
				targetDataProps.load(new FileInputStream(targetDataPath));
				targetDataProps.putAll(newCurrentDataProps);
				targetDataProps.store(new FileOutputStream(targetDataPath), null);
			} catch (Exception e) {
				logger.error("Failed to write database to disk", e);
			}

			// //update the current dataprops
			// try {
			// 	newCurrentDataProps.store(new FileOutputStream(targetDataPath), null);
			// } catch (Exception e) {
			// 	logger.error("Failed to write database to disk", e);
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


	// public static ECSNode currentServerIsRemoved(HashRing oldMeta, HashRing newMeta){
	// 	logger.info("Checking is server removed");
	// 	for ( Map.Entry<String, ECSNode> entry : newMeta.getHashRing().entrySet()) {
	// 		if(entry.getValue().getNodeName().equals(KVServer.serverName)){
	// 			return entry.getValue();
	// 		}
	// 	}
	// 	return null;
	// }

	//iterate through old metadata to see if the server's node is missing
	// public static ECSNode currentServerIsAdded(HashRing oldMeta, HashRing newMeta){
	// 	for ( Map.Entry<String, ECSNode> entry : newMeta.getHashRing().entrySet()) {
	// 		if(entry.getValue().getNodeName().equals(KVServer.serverName)){
	// 			return entry.getValue();
	// 		}
	// 	}
	// 	return null;
	// }

	// public static void moveData(){

	// }

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
			Runtime.getRuntime().addShutdownHook(new Thread(){
				public void run(){
					KVServer.ecsClient.removeKVServer(KVServer.serverName);
					KVServer.ecsClient.removeServerStatus(KVServer.serverName);
				}
			});
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
		  logger.info(this.serverNode.getIpPortHash());
		  logger.info(KVServer.metaData.getHashRing().keySet());
		  String[] hashRange = KVServer.metaData.getHashRing().get(this.serverNode.getIpPortHash()).getNodeHashRange();
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

	public String serializeHashRing() throws Exception{
		return this.serverNode.serialize();
	}

	public String getMetaDataKeyRanges(){
		return this.metaData.getSerializedHashRanges();
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