package ecs;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.lang.StringBuilder;
import java.util.Arrays;
import shared.Config;
import storage.HashUtils;
import shared.Config;
import shared.messages.IKVMessage.StatusType;

import org.apache.log4j.Logger;


//TODO: consolidate ipporthash and name
public class ECSNode implements IECSNode{

    private Logger logger = Logger.getRootLogger();
    private String name;
    private String host;
    private StatusType status;
    private int port;

    private String[] hashRange;
    private String ipPortHash;


    public ECSNode(String nodeName, String nodeHost, int nodePort){
        this(nodeName, nodeHost, nodePort, null);
    }

    public ECSNode(String nodeName, String nodeHost, int nodePort, String[] hashRange){
        verifyNodeName(nodeName);
        verifyNodeHost(nodeHost);
        verifyNodePort(nodePort);

        this.name = nodeName.trim();
        this.host = nodeHost.trim();
        this.port = nodePort;
        this.status = StatusType.SERVER_STOPPED;
        
        this.ipPortHash = HashUtils.getHashString(this.host + ":" + this.port);

        if(hashRange == null){
            this.hashRange = new String[2];
		    this.hashRange[0] = HashUtils.getHashString("");
            this.hashRange[1] = this.ipPortHash;
        }else{
            this.hashRange = hashRange;
        }
    }

    public void assignHashRange(String low, String high) throws IllegalArgumentException{
		
		if (low == null || low.isEmpty()){
            throw new IllegalArgumentException("The low value of the hash range is empty!");
        }
		if (high == null || high.isEmpty()){
            throw new IllegalArgumentException("The high value of the hash range is empty!");
        }
		
		this.hashRange = new String[2];
		this.hashRange[0] = low;
		this.hashRange[1] = high;
	}



    public String serialize() throws Exception{
		if (this.hashRange == null || this.hashRange.length != 2){
            throw new Exception("This Node's hashrange is null or has less than two entries!");
        }
        StringBuilder sb = new StringBuilder();
        sb.append(this.name);
        sb.append(Config.ECS_PROPS_DELIMITER);
        sb.append(this.host);
        sb.append(Config.ECS_PROPS_DELIMITER);
        sb.append(this.port);
        sb.append(Config.ECS_PROPS_DELIMITER);
        sb.append(this.hashRange[0]);
        sb.append(Config.ECS_PROPS_DELIMITER);
        sb.append(this.hashRange[1]);
        sb.append(Config.ECS_PROPS_DELIMITER);
        sb.append(this.status.ordinal());
        sb.append(Config.ECS_DELIMITER);
		return sb.toString();
	}

    public static ECSNode deserialize(String payload)  throws IllegalArgumentException{
		
		if (payload == null) return null;
		
		String[] arr = payload.split(Config.ECS_PROPS_DELIMITER);
		if (arr == null || arr.length < 6){
            throw new IllegalArgumentException("ECS Payload is malformed: number of arguments should be six!");
        }
		
		String name = arr[0];
		String host = arr[1];
		int port = Integer.parseInt(arr[2]);
		String[] hashRange = new String[2];
		hashRange[0] = arr[3];
		hashRange[1] = arr[4];
		int status = Integer.parseInt(arr[5]);

		ECSNode node = new ECSNode(name, host, port, hashRange);

		node.setStatus(StatusType.values()[status]);
		return node;
	}


    //TODO: Refactor
    public static Map<String, ECSNode> deserializeToECSNodeMap(String payload) throws IllegalArgumentException{
	
		if (payload == null || payload.trim().isEmpty()){
            throw new IllegalArgumentException("The payload cannot be deserialized to an ECS Node Map: payload was empty!");
        }
		
		String[] serializedArray = payload.split(Config.ECS_DELIMITER);
		if (serializedArray == null ){
            throw new IllegalArgumentException("The payload cannot be deserialized to an ECS Node Map: payload was empty!");
        }
		
		Map<String, ECSNode> nodesMap = new HashMap<>();
		for(int i = 0; i < serializedArray.length; ++i) {
            
			ECSNode node = deserialize(serializedArray[i]);
            // System.out.println("DESERIALIZED NODE NAME: "+node.getNodeHashRange()[1]);
            nodesMap.put(node.getIpPortHash(), node);
		}
		
		return nodesMap;
	}

    public static String serializeECSMap(Map<String, ECSNode> map) throws IllegalArgumentException, Exception {
        if(map == null || map.size() == 0){
            throw new IllegalArgumentException("Cannot serialize an empty map!");
        }
        StringBuilder sb = new StringBuilder();
        for(ECSNode node: map.values()){
            sb.append(node.serialize());
            // sb.append(Config.ECS_DELIMITER);            
        }
        sb.setLength(sb.length() - 1); //remove last ECS_DELIMITER
        return sb.toString();

    }


    private boolean verifyNodeName(String nodeName){
        if (nodeName == null || nodeName.trim().isEmpty()){
            logger.info(String.format("No Node name was specified!"));
            return false;
        }
        return true; 
    }

    private boolean verifyNodeHost(String nodeHost){
        if (nodeHost == null || nodeHost.trim().isEmpty()){
            logger.info(String.format("No Node Hostname was specified!"));
            return false;
        }
        return true; 
    }

    private boolean verifyNodePort(int nodePort){
        if (nodePort < 1){
            logger.info(String.format("Node port cannot be less than 1!"));
            return false;
        }
        return true; 
    }

    public void setStatus(StatusType status){
        this.status = status;
    }

        
    @Override
	public String getNodeName() {
		return this.name;
	}

	@Override
	public String getNodeHost() {
		return this.host;
	}

	@Override
	public int getNodePort() {
		return this.port;
	}

	@Override
	public String[] getNodeHashRange() {
		return this.hashRange;
	}

    public String getIpPortHash(){
        return this.ipPortHash;
    }
    
}