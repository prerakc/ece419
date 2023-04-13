package shared.communication;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;

public class KVCommunication {
    private Logger logger = Logger.getRootLogger();

    private Socket socket;
    private InputStream input;
    private OutputStream output;

    private boolean open;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    public KVCommunication(Socket socket) throws IOException {
        this.socket = socket;
        logger.info("Socket timeout: " + socket.getSoTimeout());
        output = socket.getOutputStream();
        input = socket.getInputStream();

        open = true;
    }

    public void closeConnection() {
        try {
            logger.info("Closing connection");
            open = false;
            input.close();
            output.close();
            socket.close();
        } catch (IOException ioe) {
            logger.error("Unable to close connection!", ioe);
        }
    }

    public void sendMessage(KVMessage message) throws IOException {
        byte[] serialized = message.getBytes();
        output.write(serialized, 0, serialized.length);
        output.flush();
        logger.info(String.format("Sent a message of type '%s' with key '%s' and value '%s'",
                message.getStatus().toString(), message.getKey(), message.getValue()));
    }

    public KVMessage receiveMessage() throws IOException {
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        if (read == -1) {
            throw new IOException("Input stream closed");
        }

        while (read != 13 && reading) {/* carriage return */
            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                if (msgBytes == null) {
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            bufferBytes[index] = read;
            index++;

            /* stop reading is DROP_SIZE is reached */
            if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if (msgBytes == null) {
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;
        // logger.info(String.format("Recieved message with raw bytes %s", msgBytes.toString()));
        if (Arrays.equals(msgBytes, new byte[] { 10, 31, 10 })) {
            logger.info("Got message indicating buffer out of sync!");
            return receiveMessage();
        }
        KVMessage message = new KVMessage(msgBytes);

        logger.info(String.format("Received a message of type '%s' with key '%s' and value '%s'",
                message.getStatus().toString(), message.getKey(), message.getValue()));

        return message;
    }

    public boolean isOpen() {
        return open;
    }

}
