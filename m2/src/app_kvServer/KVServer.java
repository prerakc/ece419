package app_kvServer;

import logger.LogSetup;
import shared.messages.IKVMessage.StatusType;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvECS.ECSClient;
import storage.KVStorage;
import storage.HashRing;
import ecs.ServerStatus;
import ecs.ECSNode;

import java.net.*;
import java.lang.Integer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;


public class KVServer extends Thread implements IKVServer {
	private static Logger logger = Logger.getRootLogger();

	private ServerSocket serverSocket;
	private int port;
	private int cacheSize;
	private String strategy;
	private boolean running;
	private static String serverName;
	private String address;
	private String dataDirectory = "./data";
	private String dataProperties = "database.properties";

	private KVStorage storage;

	private ArrayList<Thread> threads;
	
	private static HashRing metaData;

	private ECSNode serverNode;
	private static ECSClient ecsClient;
	private String ecsServer;
	private int ecsPort;

	public static int serverStatus;

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
	public KVServer(String address,int port, int cacheSize, String strategy, String ecsServer, int ecsPort) {
		// TODO Auto-generated method stub
		this.address = address;
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;

		this.storage = new KVStorage(dataDirectory, dataProperties);

		this.threads = new ArrayList<Thread>();

		KVServer.metaData = new HashRing();
		

		this.serverName = String.format("%s:%d",this.address,port);

		// start up ecs node
		this.serverNode = new ECSNode(this.serverName,this.address, this.port);
		this.serverNode.setStatus(StatusType.SERVER_STOPPED);

		KVServer.ecsClient = new ECSClient(ecsServer,ecsPort,false);
		KVServer.ecsClient.addKVServer(KVServer.serverName);

		KVServer.ecsClient.addStatusServerWatch(KVServer.serverName);
		KVServer.ecsClient.addMetadataServerWatch(KVServer.serverName);
	}

	public KVServer(String address, int port, int cacheSize, String strategy,String dataDir, String dataProps) {
		// TODO Auto-generated method stub
		this.address = address;
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		
		this.dataDirectory = dataDir;
		this.dataProperties = dataProps;
		this.storage = new KVStorage(dataDirectory, dataProperties);

		this.threads = new ArrayList<Thread>();

		KVServer.metaData = new HashRing();

		//this.ecsClient = new ECSClient(false);
	}

	public static void updateStatusZK(String status){
		// logger.info("++++++++++IN KVSERVER UPDATE STATUS+++++++++++");
		try{
			KVServer.serverStatus = Integer.parseInt(status);
		} catch (NumberFormatException e){
			KVServer.serverStatus = -1;
		}
	}

	public static void updateMetadataZK(String status){
		logger.info(status);
		KVServer.metaData = HashRing.getHashRingFromNodeMap(ECSNode.deserializeToECSNodeMap(status));
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

	public void deleteKV(String key) throws Exception{
		if(!inStorage(key)) {
			throw new Exception(String.format("Key '%s' is not in the database", key));
		}

		if(!storage.delete(key)) {
			throw new Exception(String.format("Failed to delete key '%s' from the database", key));
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
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run(){
				KVServer.ecsClient.removeKVServer(KVServer.serverName);
				KVServer.ecsClient.removeServerStatus(KVServer.serverName);
			}
		});
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
		String[] hashRange = this.serverNode.getNodeHashRange();
		if(hashRange[0].compareTo(hashRange[1]) < 0) //if hash range does not wrap around
			return (key.compareTo(hashRange[0]) > 0) &&  (key.compareTo(hashRange[1]) <= 0);
		return (key.compareTo(hashRange[0]) < 0) ||  (key.compareTo(hashRange[1]) > 0); //hash range does wrap around
	}

	public String serializeHashRing() throws Exception{
		return this.serverNode.serialize();
	}

	public String getMetaDataKeyRanges(){
		return this.metaData.getSerializedHashRanges();
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