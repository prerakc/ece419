package client;

import org.apache.log4j.Logger;
import shared.communication.KVCommunication;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import storage.HashUtils;
import storage.HashRing;
import shared.Config;
import ecs.ECSNode;

import java.util.*;
import java.util.ArrayList;
import java.util.Map;
import java.io.IOException;
import java.net.Socket;

public class KVStore implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();

	private String address;
	private int port;

	private Socket clientSocket;
	private KVCommunication kvCommunication;

	private HashRing metaData;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address the address of the KVServer
	 * @param port    the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		this.address = address;
		this.port = port;
		this.metaData = new HashRing();
	}

	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
		clientSocket = new Socket(address, port);
		kvCommunication = new KVCommunication(clientSocket);
		logger.info(String.format("connected to address %s at port %s", this.address, this.port));
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		logger.info("Client is closing communication!");
		if (isRunning()) {
			kvCommunication.closeConnection();
			kvCommunication = null;
		}

	}

	@Override
	public IKVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		if (!verifyKey(key)) {
			return new KVMessage(IKVMessage.StatusType.PUT_ERROR, key, value);
		}
		if (!verifyValue(value)) {
			return new KVMessage(IKVMessage.StatusType.PUT_ERROR, key, value);
		}
		KVMessage message = new KVMessage(IKVMessage.StatusType.PUT, key, value);
		kvCommunication.sendMessage(message);
		KVMessage ackMessage = kvCommunication.receiveMessage();
		boolean sentToCorrectServer = messageSentToCorrectServer(ackMessage);
		logger.info(sentToCorrectServer);
		while (!sentToCorrectServer) {
			updateConnection(key, ackMessage);
			kvCommunication.sendMessage(message);
			logger.info("LOOKING FOR SERVER");
			ackMessage = kvCommunication.receiveMessage();
			sentToCorrectServer = messageSentToCorrectServer(ackMessage);
		}
		return ackMessage;
	}

	public IKVMessage forceDelete(String key, String value) throws Exception {
		KVMessage message = new KVMessage(IKVMessage.StatusType.FORCE_DELETE, key, value);
		kvCommunication.sendMessage(message);
		KVMessage ackMessage = kvCommunication.receiveMessage();
		return ackMessage;
	}

	public IKVMessage datatransfer(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		if (!verifyKey(key)) {
			return new KVMessage(IKVMessage.StatusType.PUT_ERROR, key, value);
		}
		if (!verifyValue(value)) {
			return new KVMessage(IKVMessage.StatusType.PUT_ERROR, key, value);
		}
		KVMessage message = new KVMessage(IKVMessage.StatusType.DATATRANSFER, key, value);
		kvCommunication.sendMessage(message);
		KVMessage ackMessage = kvCommunication.receiveMessage();
		return ackMessage;
	}

	private void randomConnection() {
		ArrayList<ECSNode> servers = new ArrayList<ECSNode>();

		String thisNodeHash = HashUtils.getHashString(String.format("%s:%d", this.address, this.port));

		ECSNode primary = this.metaData.getServerForHashValue(thisNodeHash);
		ECSNode replica1 = (primary == null) ? null : this.metaData.getSuccessorNodeFromIpHash(thisNodeHash);
		ECSNode replica2 = (replica1 == null) ? null
				: this.metaData.getSuccessorNodeFromIpHash(replica1.getIpPortHash());

		if (primary != null)
			servers.add(primary);
		if (replica1 != null)
			servers.add(replica1);
		if (replica2 != null)
			servers.add(replica2);

		if (servers.size() == 0)
			return;

		Collections.shuffle(servers);

		ECSNode randomServer = servers.get(0);

		try {
			disconnect();
			this.address = randomServer.getNodeHost();
			this.port = randomServer.getNodePort();
			connect();
		} catch (Exception ex) {
			logger.error("CAN NOT CONNECT TO SERVER", ex);
		}
	}

	@Override
	public IKVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		if (!verifyKey(key)) {
			return new KVMessage(IKVMessage.StatusType.GET_ERROR, key, "");
		}
		randomConnection();
		KVMessage message = new KVMessage(IKVMessage.StatusType.GET, key, "");
		kvCommunication.sendMessage(message);
		KVMessage ackMessage = kvCommunication.receiveMessage();
		boolean sentToCorrectServer = messageSentToCorrectServer(ackMessage);
		logger.info(sentToCorrectServer);
		while (!sentToCorrectServer) {
			updateConnection(key, ackMessage);
			randomConnection();
			kvCommunication.sendMessage(message);
			logger.info("LOOKING FOR SERVER");
			ackMessage = kvCommunication.receiveMessage();
			sentToCorrectServer = messageSentToCorrectServer(ackMessage);
		}
		return ackMessage;
	}

	public IKVMessage recurGet(String key) throws Exception {
		try {
			return get(key);
		} catch (IOException e) {

			logger.error("IOException happened during get", e);
			String thisNodeHash = HashUtils.getHashString(String.format("%s:%d", this.address, this.port));

			logger.info(String.format("%s:%d", this.address, this.port));
			logger.info(this.metaData.getHashRing().keySet());
			logger.info(thisNodeHash);

			ECSNode nodeNext = this.metaData.getSuccessorNodeFromIpHash(thisNodeHash);
			this.metaData.removeServer(thisNodeHash);

			try {
				disconnect();
				this.address = nodeNext.getNodeHost();
				this.port = nodeNext.getNodePort();
				connect();
			} catch (Exception ex) {
				logger.error("CAN NOT CONNECT TO SERVER", ex);
			}
			return recurGet(key);
		}
	}

	public IKVMessage keyrange() throws Exception {
		KVMessage message = new KVMessage(IKVMessage.StatusType.KEYRANGE, "", "");
		kvCommunication.sendMessage(message);
		return kvCommunication.receiveMessage();
	}

	public IKVMessage keyrange_read() throws Exception {
		KVMessage message = new KVMessage(IKVMessage.StatusType.KEYRANGE_READ, "", "");
		kvCommunication.sendMessage(message);
		return kvCommunication.receiveMessage();
	}

	public boolean isRunning() {
		return (kvCommunication != null) && kvCommunication.isOpen();
	}

	public boolean verifyKey(String key) {
		if (key == null) {
			logger.info(String.format("Key cannot be null!"));
			return false;
		}

		if (isKeyTooBig(key)) {
			logger.info(String.format("Key is too large!"));
			return false;
		}

		if (key.isEmpty()) {
			logger.info(String.format("Key cannot be empty!"));
			return false;
		}

		return true;
	}

	public boolean verifyValue(String value) {
		if (value == null) {
			logger.info(String.format("Value cannot be null"));
			return false;
		}
		if (isValueTooBig(value)) {
			logger.info(String.format("Value is too large!"));
			return false;
		}
		return true;
	}

	public boolean isKeyTooBig(String key) {
		return key.length() >= 1024;
	}

	public boolean isValueTooBig(String value) {
		return value.length() >= 1024 * 10;
	}

	public void updateMetaData(String metaDataPayload) {
		// can put bnoth of these functions into a singular deserializeToHashRing
		try {
			Map<String, ECSNode> nodesMap = ECSNode.deserializeToECSNodeMap(metaDataPayload);
			System.out.println("******************");
			System.out.println(nodesMap.isEmpty());
			this.metaData = HashRing.getHashRingFromNodeMap(nodesMap);
		} catch (Exception e) {
			logger.error("Unable to deserialize metadata payload! \n", e);
		}
	}

	public void updateConnection(String key, KVMessage message) {
		updateMetaData(message.getValue());
		String hashValue = HashUtils.getHashString(key);
		ECSNode node = metaData.getServerForHashValue(hashValue);

		// disconnect form old server and connect to correct server
		try {
			disconnect();
			this.address = node.getNodeHost();
			this.port = node.getNodePort();
		} catch (Exception e) {

		}
		try {
			connect();
		} catch (Exception e) {

		}

		// print that connection was updated

	}

	public boolean messageSentToCorrectServer(IKVMessage message) {
		// if the server is not responsible or it is removed
		return (message.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);

		// return (message.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE ||
		// message.getStatus() == KVMessage.StatusType.SERVER_REMOVED);

	}

	public void notify(IKVMessage.StatusType status, String key, String value) throws Exception {
		KVMessage message = new KVMessage(status, key, value);
		kvCommunication.sendMessage(message);
		KVMessage ackMessage = kvCommunication.receiveMessage();
	}

	public KVMessage notify2(IKVMessage.StatusType status, String key, String value) throws Exception {
		KVMessage message = new KVMessage(status, key, value);
		kvCommunication.sendMessage(message);
		return kvCommunication.receiveMessage();
	}
}