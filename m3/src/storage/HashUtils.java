package storage;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Objects;
import java.lang.StringBuilder;
import shared.Config;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Logger;

public class HashUtils {

    private static Logger logger = Logger.getRootLogger();
    public static BigInteger mdHash(String key) {
        MessageDigest md = null;
        try{
            md = MessageDigest.getInstance("MD5");
        }catch(NoSuchAlgorithmException e){
            logger.error(e.toString());
        }
        System.out.println("*******************HASH OF KEY********************");
        // System.out.println(key);
        System.out.println(bytesToHex(md.digest(key.getBytes())));
        // System.out.println(md.digest(key.getBytes()).toString());
        BigInteger md_key = new BigInteger(1, md.digest(key.getBytes()));
        return md_key;
    }

    public static String getHashString(String key){
        // return mdHash(key).toString();
        MessageDigest md = null;
        try{
            md = MessageDigest.getInstance("MD5");
        }catch(NoSuchAlgorithmException e){
            logger.error(e.toString());
        }
        return bytesToHex(md.digest(key.getBytes()));
    }

    public static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
        sb.append(String.format("%02x", b));
    }
    return sb.toString();
}

public static String incrementHexString(String hexString) {
        // Convert hex string to integer
        long intValue = Long.parseLong(hexString, 16);
        
        // Increment integer
        intValue++;
        
        // Convert integer back to hex string
        String incrementedHexString = Long.toHexString(intValue);
        
        // If the resulting string has an odd number of characters, pad it with a leading zero
        if (incrementedHexString.length() % 2 != 0) {
            incrementedHexString = "0" + incrementedHexString;
        }
        
        return incrementedHexString;
    }

    

}