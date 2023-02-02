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
		if (isRunning()) {
			kvCommunication.closeConnection();
			kvCommunication = null;
		}

	}

	@Override
	public IKVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		if(!verifyKey(key)){
			return new KVMessage(IKVMessage.StatusType.PUT_ERROR, key, value);
		}
		if(!verifyValue(value)){
			return new KVMessage(IKVMessage.StatusType.PUT_ERROR, key, value);
		}
		KVMessage message = new KVMessage(IKVMessage.StatusType.PUT, key, value);
		kvCommunication.sendMessage(message);
		return kvCommunication.receiveMessage();
	}

	@Override
	public IKVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		if(!verifyKey(key)){
			return new KVMessage(IKVMessage.StatusType.GET_ERROR, key, "");
		}
		KVMessage message = new KVMessage(IKVMessage.StatusType.GET, key, "");
		kvCommunication.sendMessage(message);
		return kvCommunication.receiveMessage();
	}

	public boolean isRunning() {
		return (kvCommunication != null) && kvCommunication.isOpen();
	}

	public boolean verifyKey(String key){
		if (isKeyTooBig(key)){
			logger.info(String.format("Key is too large!"));
			return false;
		}

		if (key.isEmpty()){
			logger.info(String.format("Key cannot be empty!"));
			return false;
		}
		return true;
	}

	public boolean verifyValue(String value){
		if (isValueTooBig(value)){
			logger.info(String.format("Value is too large!"));
			return false;
		}
		return true;
	}

	public boolean isKeyTooBig(String key){
		return key.length() >= 1024;
	}

	public boolean isValueTooBig(String value){
		return value.length() >= 1024*10;
	}
}
