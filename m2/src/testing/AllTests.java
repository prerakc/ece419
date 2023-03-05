package testing;

import java.io.IOException;
import java.io.File;
import java.util.Objects;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import testing.TestingVars;


public class AllTests {
	private static String address = "localhost";
	private static int port = 50069;
	private static int cacheSize = 0;
	private static String strategy = "foo";
	private static String dataDir = "./data/test";
	private static String dataProps = "connection_test.properties";

	static {
		try {
			//TODO SAYING ERROR CONNECTION LOST FOR EACH TEST
			clearTestData();
			new LogSetup("logs/testing/test.log", Level.ERROR);
			new KVServer(address, port, cacheSize, strategy, dataDir, dataProps).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void clearTestData(){
		File dataDirFile = new File(dataDir);

		File[] contents = dataDirFile.listFiles();

		if (contents != null) {
			for (File dataPropsFile : dataDirFile.listFiles()) {
				dataPropsFile.delete();
			}

			dataDirFile.delete();
		}
	}
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		// clientSuite.addTestSuite(AdditionalTest.class);

		return clientSuite;
	}
	
}
