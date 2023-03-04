package storage;

import java.util.*;
import java.util.Map.Entry;
import java.math.*;
import ecs.ECSNode;
import shared.Config;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HashRing implements IHashRing {

    private static Logger logger = Logger.getRootLogger();
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

    public String getServerNameForKVKey(String kvKey) {
        String hash = HashUtils.getFixedSizeHashString(kvKey, Config.HASH_STRING_SIZE);
        String key = map.ceilingKey(hash);

        if (key == null) {
            return getFirstValue().getNodeName();
        }
        return map.get(key).getNodeName();
    }

    public ECSNode getServerForHashValue(String hashValue){
        String key = map.ceilingKey(hashValue);
        if(key == null){
            return getFirstValue();
        }
        return map.get(key);
    }

    @Override
    public String getServerNameForHashValue(String hashValue){
        String key = map.ceilingKey(hashValue);
        if(key == null){
            return getFirstValue().getNodeName();
        }
        return map.get(key).getNodeName();
    }

    public ECSNode getFirstValue(){
        String key = map.firstKey();
        if (map.firstKey() == null) {
            return null;
        }
        return map.get(key);
    }

    private void setMapFromNodeMap(Map<String, ECSNode> nodesMap){
        this.clear();
        for (String key : nodesMap.keySet()) {
            ECSNode node = nodesMap.get(key);
            //TODO: check if server is offlne before adding 
        	this.addServer(node.getIpPortHash(), node);
        }

        //update range of servers
        String prev = null;
        for (String key : this.map.keySet()) {
            ECSNode node = this.map.get(key);
            if(prev != null){
                node.assignHashRange(prev, key);
            }
            prev = key;
        }

        if(!this.map.isEmpty()){
            ECSNode firstNode = this.map.get(this.map.firstKey());
            try{
                firstNode.assignHashRange(prev, this.map.firstKey());
            }catch (IllegalArgumentException e) {
			    logger.error("Assinging Hash Ranges from given nodemap failed!", e);
		    }
        }

    } 

    public static HashRing getHashRingFromNodeMap(Map<String, ECSNode> nodesMap) {
    	    	
        HashRing ring = new HashRing();
        ring.setMapFromNodeMap(nodesMap);

        return ring;
    }

}