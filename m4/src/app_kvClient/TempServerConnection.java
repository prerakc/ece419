package app_kvClient;

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
public class TempServerConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;

	private Socket clientSocket;

	private NotificationServer server;

	private KVCommunication kvCommunication;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public TempServerConnection(Socket clientSocket, NotificationServer server) throws IOException {
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
			case NOTIFICATION:
				if (value.equals("null") || value.isEmpty()) {
					System.out.println(String.format("Notification> " + "Key '%s' has been deleted", key));
				} else {
					System.out.println(String.format("Notification> " + "Value of key '%s' has been changed to '%s'", key, value));
				}
				responseStatus = StatusType.NOTIFICATION_ACK;
				break;
			default:
				logger.info("Unexpected message status: " + status);
				responseStatus = status;
				break;
		}

		return new KVMessage(responseStatus, key, responseValue);
	}
}