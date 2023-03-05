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

    public static String getFixedSizeHashString(String key, int stringSize){
        String hash = mdHash(key).toString();
        if (stringSize <= hash.length()){
            return hash;
        }
        //TODO REMEMBER NOT USING UNFIXED HASHSIZE
        return hash;
        // return padStringLeftToSize(hash, Config.HASH_STRING_SIZE);
    }

    public static String padStringLeftToSize(String s, int size){
        if (s == null || size < 0) 
            return s;
        if(size  < s.length())
            return s.substring(0, size);
        
        char[] paddingCharArr = new char[size - s.length()];
        for(int i=0; i<paddingCharArr.length; i++){
            paddingCharArr[i] = Config.PAD_CHAR;
        }
        String padded = paddingCharArr.toString() + s;
        return padded;
    }

    

}