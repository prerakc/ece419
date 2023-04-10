package shared.messages;

import org.apache.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class KVMessage implements IKVMessage {
    private Logger logger = Logger.getRootLogger();

    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    private static final char DELIMITER = 0x1F;

    private StatusType status;
    private String key;
    private String value;

    private byte[] serialized;

    public KVMessage(StatusType status, String key, String value) {
        this.status = status;
        this.key = key;
        this.value = value;

        String messageString = status.toString() + DELIMITER + key + DELIMITER + value + DELIMITER;

        byte[] messageBytes = messageString.getBytes(StandardCharsets.UTF_8);
        byte[] controlBytes = new byte[] { LINE_FEED, RETURN };
        byte[] tmp = new byte[messageBytes.length + controlBytes.length];

        System.arraycopy(messageBytes, 0, tmp, 0, messageBytes.length);
        System.arraycopy(controlBytes, 0, tmp, messageBytes.length, controlBytes.length);

        serialized = tmp;
    }

    public KVMessage(byte[] serialized) {
        logger.info("Raw bytes: " + Arrays.toString(serialized));
        String messageString = new String(serialized, StandardCharsets.UTF_8);

        String[] fields = messageString.split(String.valueOf(DELIMITER), 4);
        logger.info("Message: " + messageString);
        this.status = StatusType.valueOf(fields[0].toUpperCase());
        this.key = fields[1];
        this.value = fields[2];

        byte[] controlBytes = new byte[] { LINE_FEED, RETURN };
        byte[] tmp = new byte[serialized.length + controlBytes.length];

        System.arraycopy(serialized, 0, tmp, 0, serialized.length);
        System.arraycopy(controlBytes, 0, tmp, serialized.length, controlBytes.length);

        this.serialized = tmp;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

    public byte[] getBytes() {
        return serialized;
    }
}
