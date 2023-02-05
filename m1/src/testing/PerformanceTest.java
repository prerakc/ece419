package testing;

import java.io.IOException;
import java.io.File;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import client.KVStore;
import logger.LogSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

public class PerformanceTest {
	private static String dataDir = "./data/test";
	private static String dbName = "test_performance.properties";
    private static String logLocation = "./logs/performance.log";

	

	static {
		try {
			clearTestData();
			new LogSetup(logLocation, Level.ERROR);
			new KVServer(50000, 10, "FIFO",dataDir,dbName).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void clearTestData(){
		File myObj = new File(dataDir +"/"+ dbName); 
		myObj.delete();
	}

	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Performance Test Suite");
		clientSuite.addTestSuite(PerformanceFunctionsTest.class);
		return clientSuite;
	}
		
    
}

