package testing;

import app_kvServer.KVServer;
import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

public class DistributedKVServerPerformanceTest extends TestCase {

	private KVStore kvClient;

	private static String address = "localhost";
	private static int port = 50071;
	private static int cacheSize = 0;
	private static String strategy = "foo";
	private static String dataDir = "./data/test";
	private static String dataProps = "non_distributed_kvserver_performance_test.properties";
	int[] clientCounts = { 1, 5, 20, 50, 100 };
	// int[] serverCounts = {1, 5, 10, 50, 100};
	int[] serverCounts = { 5 };
	KVStore[] clientList;

	int sCount = 1;
	int cCount = 50;
	int notifCount = 0;
	
	public void runPerformance(int putNumber, int getNumber) {
		Exception ex = null;

		String baseKey = "PERFORMANCE_BASELINE_KEY_";
		String baseValue = "PERFORMANCE_BASELINE_VALUE_";

		for (int i = 0; i < putNumber; i++) {
			try {
				Thread.sleep(750);
			} catch (InterruptedException e) {
				System.out.println(e);
			}
			try {
				clientList[i % cCount].put(baseKey + Integer.toString(i), baseValue + Integer.toString(i));
			} catch (Exception e) {
				ex = e;
			}
		}
		for (int i = 0; i < getNumber; i++) {
			try {
				Thread.sleep(750);
			} catch (InterruptedException e) {
				System.out.println(e);
			}
			try {
				clientList[i % cCount].get(baseKey + Integer.toString(i));
			} catch (Exception e) {
				ex = e;
			}
		}
	}

	public void connectClient() {

		clientList = new KVStore[cCount];
		for (int i = 0; i < cCount; i++) {
			clientList[i] = new KVStore(TestingVars.SERVER_A_ADDRESS,
					TestingVars.PORT_ARRAY[i % (TestingVars.PORT_ARRAY.length-1)]);
			try {
				clientList[i].connect();
			} catch (Exception ignored) {
				System.out.println(TestingVars.PORT_ARRAY[i % (TestingVars.PORT_ARRAY.length-1)]);
				System.out.println("errrorrrrorr");
				System.out.println(ignored);
			}
		}
	}

	public void disconnectClient() {
		for (int i = 0; i < cCount; i++) {
			try {
				clientList[i].disconnect();
			} catch (Exception ignored) {
				System.out.println("errrorrrrorr");
				System.out.println(ignored);
			}
		}
	}

	@Test
	public void testPerformance() {
		int totalTest = 10;
		int putNumber;
		int getNumber;

		long startTime;
		long endTime;

		// int serverCount = 1;
		for (int serverCount : serverCounts) {

			System.out.println(String.format("====== CLIENT COUNT: %d,  SERVER COUNT: %d ======", cCount, serverCount));
			// 80% puts, 20% gets
			putNumber = (int) (totalTest * 0.8);
			getNumber = totalTest - putNumber;

			TestingUtils.spinUpServers(serverCount, dataDir, dataProps);
			connectClient();

			// enables notififcation to client
			for (int i=0;i<notifCount;i++){
				try{
					clientList[0].notify2(IKVMessage.StatusType.NOTIFICATION_TOGGLE,
								String.format("%s:%d", "127.0.0.1", TestingVars.PORT_ARRAY[i % (TestingVars.PORT_ARRAY.length-1)]), "subscribe");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
							
			startTime = System.currentTimeMillis();
			runPerformance(putNumber, getNumber);
			endTime = System.currentTimeMillis();
			System.out.println("+++ 80% puts, 20% gets: " + (endTime - startTime) + " ms +++");

			disconnectClient();
			TestingUtils.spinDownServers();
			AllTests.clearTestData();
			// 50%/50%
			putNumber = (int) (totalTest * .5);
			getNumber = totalTest - putNumber;

			TestingUtils.spinUpServers(serverCount, dataDir, dataProps);
			connectClient();
			startTime = System.currentTimeMillis();
			runPerformance(putNumber, getNumber);
			endTime = System.currentTimeMillis();
			System.out.println("+++ 50% puts, 50% gets: " + (endTime - startTime) + " ms +++");

			disconnectClient();
			TestingUtils.spinDownServers();
			AllTests.clearTestData();
			// 80% gets, 20% puts
			putNumber = (int) (totalTest * .2);
			getNumber = totalTest - putNumber;

			TestingUtils.spinUpServers(serverCount, dataDir, dataProps);
			connectClient();
			startTime = System.currentTimeMillis();
			runPerformance(putNumber, getNumber);
			endTime = System.currentTimeMillis();
			System.out.println("+++ 20% puts, 80% gets: " + (endTime - startTime) + " ms +++");

			disconnectClient();
			TestingUtils.spinDownServers();
			AllTests.clearTestData();

		}

		// SAMPLE TESTING ARRAY LIST READ
		// Map<String, String> db;
		// db = Collections.synchronizedMap(new HashMap<String, String>());
		// db.put("Test", "Test");
		// startTime = System.nanoTime();
		// db.get("Test");
		// endTime = System.nanoTime();
		// System.out.println("ARRAY MAP READ TIMING " + (endTime - startTime) + " ns
		// +++");
	}
}
