package app_kvServer;

import org.apache.log4j.Logger;
import shared.communication.KVCommunication;
import shared.messages.IKVMessage.StatusType;
import shared.messages.KVMessage;

import java.io.IOException;
import java.net.Socket;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class TempClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;

	private Socket clientSocket;

	private KVServer server;

	private KVCommunication kvCommunication;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public TempClientConnection(Socket clientSocket, KVServer server) throws IOException {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.server = server;
		this.kvCommunication = new KVCommunication(clientSocket);
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		while (isOpen) {
			try {
				KVMessage latestMsg = kvCommunication.receiveMessage();
				KVMessage responseMsg = handleMessage(latestMsg);
				kvCommunication.sendMessage(responseMsg);
			/* connection either terminated by the client or lost due to
			 * network problems*/
			} catch (IOException ioe) {
				logger.error("Error! Connection lost!", ioe);
				isOpen = false;
			}
		}

		kvCommunication.closeConnection();
	}

	private KVMessage handleMessage(KVMessage message) {
		StatusType status = message.getStatus();
		String key = message.getKey();
		String value = message.getValue();

		KVMessage responseMessage = null;

		try {
			switch (status) {
				case GET:
					if (server.inStorage(key)) {
						value = server.getKV(key);
						responseMessage = new KVMessage(StatusType.GET_SUCCESS, key, value);
					} else {
						responseMessage = new KVMessage(StatusType.GET_ERROR, key, value);
					}
					break;
				case PUT:
					if (server.inStorage(key)) {
						if (value.isEmpty()) {
							server.putKV(key, value);
							responseMessage = new KVMessage(StatusType.DELETE_SUCCESS, key, value);
						} else {
							server.putKV(key, value);
							responseMessage = new KVMessage(StatusType.PUT_UPDATE, key, value);
						}
					} else {
						if (value.isEmpty()) {
							responseMessage = new KVMessage(StatusType.DELETE_ERROR, key, value);
						} else {
							server.putKV(key, value);
							responseMessage = new KVMessage(StatusType.PUT_SUCCESS, key, value);
						}
					}
					break;
				default:
					logger.info("Unexpected message of status " + message.getStatus().toString());
					responseMessage = message;
					break;
			}
		} catch (Exception e) {

		}

		return responseMessage;
	}
}
