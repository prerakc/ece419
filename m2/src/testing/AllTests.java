package testing;

import java.io.IOException;
import java.io.File;

import org.apache.log4j.Level;

import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	private static String dataDir = "./data/test";

	static {
		try {
			//TODO SAYING ERROR CONNECTION LOST FOR EACH TEST
			clearTestData();
			new LogSetup("logs/testing/test.log", Level.ERROR);
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
		clientSuite.addTestSuite(NonDistributedTest.class);

		return clientSuite;
	}
	
}
