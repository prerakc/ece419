package ecs;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.lang.StringBuilder;

import shared.Config;
import storage.HashUtils;
import shared.Config;
import shared.messages.IKVMessage.StatusType;

import org.apache.log4j.Logger;



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
        this.status = StatusType.SERVER_NOT_AVAILABLE;


        if(hashRange == null){
            this.hashRange = new String[2];
		    this.hashRange[0] = HashUtils.padStringLeftToSize("", Config.HASH_STRING_SIZE);
            this.hashRange[1] = HashUtils.getFixedSizeHashString(this.ipPortHash, Config.HASH_STRING_SIZE);
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
        sb.append(this.port);
        sb.append(this.hashRange[0]);
        sb.append(this.hashRange[1]);
        sb.append(this.status);
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
            throw new IllegalArgumentException("The payload cannot be deserialized to an ECS Node Map: payload was empty!");;
        }
		
		String[] serializedArray = payload.split(Config.ECS_DELIMITER);
		if (serializedArray == null ){
            throw new IllegalArgumentException("The payload cannot be deserialized to an ECS Node Map: payload was empty!");
        }
		
		Map<String, ECSNode> nodesMap = new HashMap<>();
		for(int i = 1; i < serializedArray.length; ++i) {
			ECSNode node = deserialize(serializedArray[i]);
            nodesMap.put(node.name, node);
		}
		
		return nodesMap;
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