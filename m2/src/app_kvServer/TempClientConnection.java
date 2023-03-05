package app_kvServer;

import org.apache.log4j.Logger;
import shared.communication.KVCommunication;
import shared.messages.IKVMessage.StatusType;
import shared.messages.KVMessage;
import ecs.ECSNode;

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

		StatusType responseStatus = StatusType.SERVER_NOT_AVAILABLE;
		String responseValue = value;

		switch (status) {
			case GET:
				try {
					logger.info(server.getStatus());
					logger.info(StatusType.SERVER_IDLE);
					logger.info(server.getStatus() != StatusType.SERVER_IDLE);
					
					if(server.getStatus() != StatusType.SERVER_IDLE || server.getStatus() != StatusType.SERVER_WRITE_LOCK){
						if(server.getStatus() == StatusType.SERVER_STOPPED){
							responseStatus = StatusType.SERVER_NOT_RESPONSIBLE;
							responseValue = this.server.serializeMetaData();
							logger.info(String.format("Server is not available. Sending back metadata"));
						} else {
							// should never be hit
							responseStatus = StatusType.SERVER_STOPPED;
							responseValue = this.server.serializeMetaData();
						}
					}
					else if(!server.isResponsibleForRequest(key)){
						responseStatus = StatusType.SERVER_NOT_RESPONSIBLE;
						// responseValue = server.serializeHashRing();
						responseValue = this.server.serializeMetaData();
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
				if(server.getStatus() != StatusType.SERVER_IDLE){
					if(server.getStatus() == StatusType.SERVER_STOPPED){
						responseStatus = StatusType.SERVER_NOT_RESPONSIBLE;
						responseValue = this.server.serializeMetaData();
						logger.info(String.format("Server is not available. Sending back metadata"));
					}else if(server.getStatus() == StatusType.SERVER_WRITE_LOCK){
						responseStatus = StatusType.SERVER_NOT_RESPONSIBLE;
						responseValue = this.server.serializeMetaData();
						logger.info("Server cannot be written to. Try request again in a few minutes.");
					}
				}else if(!server.isResponsibleForRequest(key)){
					responseStatus = StatusType.SERVER_NOT_RESPONSIBLE;
					responseValue = this.server.serializeMetaData();
					logger.info(String.format("Server cannot service for key '%s'. Sending back metadata", key));
				}
				else if (value.equals("null") || value.isEmpty()) {
					try {
						server.deleteKV(key);
						responseStatus = StatusType.DELETE_SUCCESS;
						logger.info(String.format("Deleted key '%s' from the database", key));
					} catch (Exception e) {
						responseStatus = StatusType.DELETE_ERROR;
						logger.error(e.getMessage());
					}
				} else {
					boolean existingKey = server.inStorage(key);
					try {
						if(!server.isResponsibleForRequest(key)){
							responseStatus = StatusType.SERVER_NOT_RESPONSIBLE;
							responseValue = server.serializeHashRing();
							logger.info(String.format("Server cannot service for key '%s'. Sending back metadata", key));
						}else{
							server.putKV(key, value);
							if (existingKey) {
								responseStatus = StatusType.PUT_UPDATE;
								logger.info(String.format("Updated value associated with key '%s' to '%s'", key, value));
							} else {
								responseStatus = StatusType.PUT_SUCCESS;
								logger.info(String.format("Added key '%s' and value '%s' to the database", key, value));
							}
						}
					} catch (Exception e) {
						responseStatus = StatusType.PUT_ERROR;
						logger.error(e.getMessage());
					}
				}
				break;
			case KEYRANGE:
				responseValue = server.getMetaDataKeyRanges();
				responseStatus = StatusType.KEYRANGE_SUCCESS;
				break;
			default:
				logger.info("Unexpected message status: " + status);
				responseStatus = status;
				break;
		}

		return new KVMessage(responseStatus, key, responseValue);
	}
}
