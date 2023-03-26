package testing;

import java.io.IOException;
import java.io.File;

import org.apache.log4j.Level;

import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			//TODO SAYING ERROR CONNECTION LOST FOR EACH TEST
			clearTestData();
			new LogSetup("logs/testing/test.log", Level.ERROR);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void helper(String dirPath) {
		File dataDirFile = new File(dirPath);

		File[] contents = dataDirFile.listFiles();

		if (contents != null) {
			for (File dataPropsFile : dataDirFile.listFiles()) {
				dataPropsFile.delete();
			}

			dataDirFile.delete();
		}

		return;
	}

	public static void clearTestData(){
		helper("./data/");
		// helper("./data");
	}
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");

//		clientSuite.addTestSuite(NonDistributedKVStoreTest.class);
//		clientSuite.addTestSuite(NonDistributedKVServerTest.class);
//		clientSuite.addTestSuite(NonDistributedKVServerPerformanceTest.class);
		// clientSuite.addTestSuite(DistributedKVStoreTest.class);
		// clientSuite.addTestSuite(HashRingTests.class);
		clientSuite.addTestSuite(DistributedKVServerPerformanceTest.class);
		// clientSuite.addTestSuite(DistributedKVMultiClient.class);
		// clientSuite.addTestSuite(DistributedKVServerTest.class);
		return clientSuite;
	}
}
