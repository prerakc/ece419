package testing;

import java.io.IOException;
import java.io.File;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {
	private static String dataDir = "./data/test";
	private static String dbName = "test.properties";

	static {
		try {
			//TODO SAYING ERROR CONNECTION LOST FOR EACH TEST
			clearTestData();
			new LogSetup("logs/testing/test.log", Level.ERROR);
			new KVServer(50000, 10, "FIFO",dataDir,dbName).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void clearTestData(){
		File myObj = new File(dataDir +"/"+ dbName); 
		myObj.delete();
	}
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class); 
		clientSuite.addTestSuite(AdditionalTest.class);

		return clientSuite;
	}
	
}
