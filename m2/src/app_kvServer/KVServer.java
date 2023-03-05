package app_kvServer;

import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import storage.KVStorage;
import storage.HashRing;
import ecs.ECSNode;

import java.net.*;

import java.io.IOException;
import java.util.*;

public class KVServer extends Thread implements IKVServer {
	private static Logger logger = Logger.getRootLogger();

	private ServerSocket serverSocket;
	private int port;
	private int cacheSize;
	private String strategy;
	private boolean running;
	private String serverName;

	private String dataDirectory;
	private String dataProperties;

	private KVStorage storage;

	private ArrayList<Thread> threads;
	
	private HashRing metaData;

	private ECSNode serverNode;


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
	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this(port, cacheSize, strategy, "./data", String.format("%s_%s.properties", "localhost", port));
	}

	public KVServer(int port, int cacheSize, String strategy, String dataDir, String dataProps) {
		// TODO Auto-generated method stub
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;

		this.serverName = String.format("%s_%s", "localhost", this.port);

		this.dataDirectory = dataDir;
		this.dataProperties = dataProps;

		this.storage = new KVStorage(dataDirectory, dataProperties);

		this.threads = new ArrayList<Thread>();

		this.metaData = new HashRing();

		// start up ecs node
		this.serverNode = new ECSNode(serverName, this.getHostname(), this.port);
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
		// see if key is between the hash range of the ECSNode
		String[] hashRange = this.serverNode.getNodeHashRange();
		return (key.compareTo(hashRange[0]) >= 0) &&  (key.compareTo(hashRange[1]) <= 1);
	}

	public String serializeHashRing() throws Exception{
		return this.serverNode.serialize();
	}
	

	public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.ALL);
			if(args.length != 3) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <cachesize> <cachetype>!");
			} else {
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				new KVServer(port, cacheSize, strategy).start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port> <cachesize> <cachetype>!");
			System.exit(1);
		}
	}
}