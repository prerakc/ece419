package testing;

import java.util.Map;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import java.io.*;
import java.util.*;

public class PerformanceFunctionsTest extends TestCase{

	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}

    public void runPerformance(int putNumber, int getNumber){
        Exception ex = null;
        String baseKey = "PERFORMANCE_BASELINE_KEY_";
		String baseValue = "PERFORMANCE_BASELINE_VALUE_";

        for(int i=0; i<putNumber;i++){
			try {
				kvClient.put(baseKey + Integer.toString(i), baseValue + Integer.toString(i));
			} catch (Exception e) {
				ex = e;
			}
		}
		for(int i=0; i<getNumber;i++){
			try {
				kvClient.get(baseKey + Integer.toString(i));
			} catch (Exception e) {
				ex = e;
			}
		}
    }

	@Test
	public void testPerformance(){
		
		int totalTest = 5000;
		int putNumber;
		int getNumber;

        long startTime;
        long endTime;

		

		//  80% puts, 20% gets
		putNumber = (int)(totalTest * 0.8);
		getNumber = totalTest - putNumber;
        
        startTime = System.currentTimeMillis();
		runPerformance(putNumber,getNumber);
        endTime = System.currentTimeMillis();
        System.out.println("+++ 80% puts, 20% gets: " + (endTime - startTime) + " ms +++");

		// 50%/50%
		putNumber = (int)(totalTest * .5);
		getNumber = totalTest - putNumber;

        startTime = System.currentTimeMillis();
		runPerformance(putNumber,getNumber);
        endTime = System.currentTimeMillis();
        System.out.println("+++ 50% puts, 50% gets: " + (endTime - startTime) + " ms +++");

		// 80% gets, 20% puts 
		putNumber = (int)(totalTest * .2);
		getNumber = totalTest - putNumber;

        startTime = System.currentTimeMillis();
		runPerformance(putNumber,getNumber);
        endTime = System.currentTimeMillis();
        System.out.println("+++ 20% puts, 80% gets: " + (endTime - startTime) + " ms +++");

		// SAMPLE TESTING ARRAY LIST READ
		// Map<String, String> db;
		// db = Collections.synchronizedMap(new HashMap<String, String>());
		// db.put("Test", "Test");
		// startTime = System.nanoTime();
		// db.get("Test");
        // endTime = System.nanoTime();
        // System.out.println("ARRAY MAP READ TIMING " + (endTime - startTime) + " ns +++");
	}
}
