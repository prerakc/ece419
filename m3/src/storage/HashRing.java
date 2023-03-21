package storage;

import java.util.*;
import java.math.*;
import java.lang.StringBuilder;
import ecs.ECSNode;
import shared.Config;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Map.Entry;
import java.util.Arrays;

public class HashRing {

    private static Logger logger = Logger.getRootLogger();
    //TODO:  may need to swtich to different ordered map structure (conncurrent skiplist map)
    private TreeMap<String, ECSNode> map;

    public HashRing() {
        this.map = new TreeMap<String, ECSNode>();
    }

    public TreeMap<String, ECSNode> getHashRing() {
        return map;
    }

    public void addServer(String hashValue, ECSNode server) {
        map.put(hashValue, server);
    }
    public void removeServer(String hashValue) {
        map.remove(hashValue); 
    }

    public void clear() {
        this.map.clear();
    }

    public ECSNode getServerForHashValue(String hashValue){
        String key = map.ceilingKey(hashValue);
        return (key == null ? getFirstValue() : map.get(key));
    }

    public ECSNode getServerForKVKey(String kvKey) {
        String hash = HashUtils.getHashString(kvKey);
        return getServerForHashValue(hash);
    }

    public String getServerNameForHashValue(String hashValue){
        ECSNode node = getServerForHashValue(hashValue);
        return (node == null) ? null : node.getNodeName();
    }

    public String getServerNameForKVKey(String kvKey) {
        ECSNode node = getServerForKVKey(kvKey);
        return (node == null) ? null : node.getNodeName();
    }

    public ECSNode getFirstValue(){
        try {
            String key = map.firstKey();
            return (key == null ? null : map.get(key));
        } catch (NoSuchElementException nse) {
            return null;
        }
    }

    private void setMapFromNodeMap(Map<String, ECSNode> nodesMap){
        this.clear();
        for (String key : nodesMap.keySet()) {
            ECSNode node = nodesMap.get(key);
            //TODO: check if server is offlne before adding 
        	this.addServer(node.getIpPortHash(), node);
        }

        // //update range of servers
        // String prev = null;
        // for (String key : this.map.keySet()) {
        //     ECSNode node = this.map.get(key);
        //     if(prev != null){
        //         node.assignHashRange(prev, key);
        //     }
        //     prev = key;
        // }

        // if(!this.map.isEmpty()){
        //     ECSNode firstNode = this.map.get(this.map.firstKey());
        //     try{
        //         firstNode.assignHashRange(prev, this.map.firstKey());
        //     }catch (IllegalArgumentException e) {
		// 	    logger.error("Assinging Hash Ranges from given nodemap failed!", e);
		//     }
        // }

    } 

    public static HashRing getHashRingFromNodeMap(Map<String, ECSNode> nodesMap) {
    	    	
        HashRing ring = new HashRing();
        ring.setMapFromNodeMap(nodesMap);

        return ring;
    }

    public String getSerializedHashRanges(){
        StringBuilder sb = new StringBuilder();
        for (String key : map.keySet()) {
            ECSNode node = map.get(key);
            String[] hashRange = node.getNodeHashRange();
            sb.append(hashRange[0]);
            sb.append(Config.ECS_PROPS_DELIMITER);
            sb.append(hashRange[1]);
            sb.append(Config.ECS_PROPS_DELIMITER);
            sb.append("<" + node.getNodeHost() + ":" + node.getNodePort() + ">");
            sb.append(Config.ECS_DELIMITER);
        }
        sb.append("\r\n");
        return sb.toString();
    }


    public ECSNode getPredecessorNodeFromIpHash(String serverHash){
        //handle name is null

        //handle map is null
        if(this.map.size() < 2)
            return null;

        ECSNode node = this.map.get(serverHash);
        Entry<String, ECSNode> predecessorEntry = this.map.lowerEntry(serverHash);
        predecessorEntry = predecessorEntry == null ? this.map.lastEntry() : predecessorEntry;
        return (predecessorEntry == null ? null : predecessorEntry.getValue());
    }

    public ECSNode getSuccessorNodeFromIpHash(String serverHash){
        //handle name is null

        //handle map is null
        if(this.map.size() < 2)
            return null;

        ECSNode node = this.map.get(serverHash);
        Entry<String, ECSNode> successorEntry = this.map.higherEntry(serverHash);
        successorEntry = (successorEntry == null ? this.map.firstEntry() : successorEntry);
        return (successorEntry == null ? null : successorEntry.getValue());
    }

    //check that node is one of the two next successors of coordinator
    public boolean isReplicaOfNode(ECSNode coordinator, ECSNode node){ 
        if(coordinator == null || node == null){
            logger.info("Arguments of isReplicaOfNode cannot be null!");
            return false;
        }

        ECSNode successor = getSuccessorNodeFromIpHash(coordinator.getIpPortHash());
        if(successor == null) return false;
        if(successor == node) return true;

        successor = getSuccessorNodeFromIpHash(coordinator.getIpPortHash());
        return successor == node;
    }


    public static boolean keyInRange(String key, String[] hashRange){
		key = HashUtils.getHashString(key);

        logger.info("Checking if key is in specified hash range");
        logger.info("Key: " + key);
        logger.info("HashRange: " + Arrays.toString(hashRange));

		if(hashRange[0].compareTo(hashRange[1]) < 0){//if hash range does not wrap around 
			return (key.compareTo(hashRange[0]) > 0) &&  (key.compareTo(hashRange[1]) <= 0); // lower hash is bigger than key and higher hash is lower
		}
		return (key.compareTo(hashRange[0]) > 0) ||  (key.compareTo(hashRange[1]) <= 0); //hash range does wrap around
	}

    public static String incrementHexString(String hexString) {
        // Convert hex string to integer
        // int intValue = Integer.parseInt(hexString, 16);
        BigInteger bi = new BigInteger(hexString, 16);
        
        // Increment integer
        bi = bi.add(BigInteger.ONE);
        
        // Convert integer back to hex string
        String incrementedHexString = bi.toString(16);
        
        // If the resulting string has an odd number of characters, pad it with a leading zero
        if (incrementedHexString.length() % 2 != 0) {
            incrementedHexString = "0" + incrementedHexString;
        }
        
        return incrementedHexString;
    }

    // public void removeEntriesBetweenrange(String low, String high){
    //     Map<String, ECSNode> toBeRemoved = this.getEntriesBetweenRange(low, high);
    //     for(String key: toBeRemoved.keySet()){
    //         this.map.remove(key);
    //     }
    // }

    // public Map getEntriesBetweenRange(String low, String high){
    //     return this.map.subMap(low, true, high, true);
    // }

}