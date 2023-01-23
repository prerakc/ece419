package client;

import org.apache.log4j.Logger;
import shared.communication.KVCommunication;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.net.Socket;

public class KVStore implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();

	private String address;
	private int port;

	private Socket clientSocket;
	private KVCommunication kvCommunication;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		this.address = address;
		this.port = port;
	}

	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
		clientSocket = new Socket(address, port);
		kvCommunication = new KVCommunication(clientSocket);
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		if (kvCommunication != null) {
			kvCommunication.closeConnection();
			kvCommunication = null;
		}

	}

	@Override
	public IKVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		KVMessage message = new KVMessage(IKVMessage.StatusType.PUT, key, value);
		kvCommunication.sendMessage(message);
		return kvCommunication.receiveMessage();
	}

	@Override
	public IKVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		KVMessage message = new KVMessage(IKVMessage.StatusType.GET, key, "");
		kvCommunication.sendMessage(message);
		return kvCommunication.receiveMessage();
	}

	public boolean isRunning() {
		return (kvCommunication != null) && kvCommunication.isOpen();
	}
}
