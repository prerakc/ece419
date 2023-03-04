package app_kvServer;

import org.apache.log4j.Logger;
import shared.communication.KVCommunication;
import shared.messages.IKVMessage.StatusType;
import shared.messages.KVMessage;
import ecs.ServerStatus;

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
				logger.error("Error! Connection lost!");
				isOpen = false;
			}
		}

		kvCommunication.closeConnection();
	}

	private KVMessage handleMessage(KVMessage message) {
		StatusType status = message.getStatus();
		String key = message.getKey();
		String value = message.getValue();

		StatusType responseStatus;
		String responseValue = value;

		switch (status) {
			case GET:
				try {
					if(!server.isResponsibleForRequest(key)){
						responseStatus = StatusType.SERVER_NOT_RESPONSIBLE;
						responseValue = server.serializeHashRing();
						logger.info(String.format("Server cannot service for key '%s'. Sending back metadata", key));
					}else{
						responseValue = server.getKV(key);
						responseStatus = StatusType.GET_SUCCESS;
						logger.info(String.format("Server cannot service for key '%s'. Sending back metadata", key));
					}
				} catch (Exception e) {
					responseStatus = StatusType.GET_ERROR;
					logger.error(e.getMessage());
				}
				break;
			case PUT:
				if (value.equals("null") || value.isEmpty()) {
					try {
						if(!server.isResponsibleForRequest(key)){
							responseStatus = StatusType.SERVER_NOT_RESPONSIBLE;
							responseValue = server.serializeHashRing();
							logger.info(String.format("Server cannot service for key '%s'. Sending back metadata", key));
						}else{
							server.deleteKV(key);
							responseStatus = StatusType.DELETE_SUCCESS;
							logger.info(String.format("Deleted key '%s' from the database", key));
						}
					} catch (Exception e) {
						responseStatus = StatusType.DELETE_ERROR;
						logger.error(e.getMessage());
					}
				} else {
					boolean existingKey = server.inStorage(key);
					try {
						server.putKV(key, value);
						if (existingKey) {
							responseStatus = StatusType.PUT_UPDATE;
							logger.info(String.format("Updated value associated with key '%s' to '%s'", key, value));
						} else {
							responseStatus = StatusType.PUT_SUCCESS;
							logger.info(String.format("Added key '%s' and value '%s' to the database", key, value));
						}
					} catch (Exception e) {
						responseStatus = StatusType.PUT_ERROR;
						logger.error(e.getMessage());
					}
				}
				break;
			default:
				logger.info("Unexpected message status: " + status);
				responseStatus = status;
				break;
		}

		return new KVMessage(responseStatus, key, responseValue);
	}
}
