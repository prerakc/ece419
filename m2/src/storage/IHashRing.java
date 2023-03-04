package storage;

import ecs.ECSNode;

public interface IHashRing {
	
	public String getServerNameForKVKey(String kvKey); 
	
	public String getServerNameForHashValue(String hashValue);
	 
    
}