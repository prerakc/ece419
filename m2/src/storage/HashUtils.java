package storage;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Objects;
import shared.Config;

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
        BigInteger md_key = new BigInteger(1, md.digest(key.getBytes()));
        return md_key;
    }

    public static String getHashString(String key){
        return mdHash(key).toString();
    }

    public static String getHashString(String key, int stringSize){
        String hash = mdHash(key).toString();
        //TODO REMEMBER NOT USING UNFIXED HASHSIZE
        return hash;
        // return padStringLeftToSize(hash, Config.HASH_STRING_SIZE);
    }

    

}