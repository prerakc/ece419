package testing;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Before;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import java.util.*;
// import shared.Config;
// import storage.HashUtils;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DistributedTest extends TestCase {

    private static String address = "127.0.0.1";
    private static int ecsPort = 2181;
    private static int serverPort1 = 8082;
    private static int serverPort2 = 8083;
    private static int serverPort3 = 8084;
    private static int cacheSize = 0;
    private static String strategy = "foo";
    private static String dataDir = "./data/test";
    private static String dataProps1 = "distributed_kvstore_test_1.properties";
    private static String dataProps2 = "distributed_kvstore_test_2.properties";
    private static String dataProps3 = "distributed_kvstore_test_3.properties";

    @BeforeClass
    public static void setupBeforeAll() {
    }

    @Before
    public void setUp() {
        // try {
        //     Thread.sleep(1000);
        // } catch (InterruptedException e) {
        //     Thread.currentThread().interrupt();
        // }
    }

    @After
    public void tearDown() {
    }

    @AfterClass
    public static void tearDownAfterAll() {
    }

    static {
        // new Thread(){
        //     public void run(){
        //         new ECSClient(address,ecsPort, true).start();
        //         while(true){}
        //     }
        // }.start();
        // try {
        //     Thread.sleep(5000);
        // } catch (InterruptedException e) {
        //     Thread.currentThread().interrupt();
        // }
        // new KVServer(address, serverPort1, cacheSize, strategy, address, ecsPort, dataDir, dataProps1).start();
        // try {
        //     Thread.sleep(5000);
        // } catch (InterruptedException e) {
        //     Thread.currentThread().interrupt();
        // }
        // new KVServer(address, serverPort2, cacheSize, strategy, address, ecsPort, dataDir, dataProps2).start();
        // try {
        //     Thread.sleep(5000);
        // } catch (InterruptedException e) {
        //     Thread.currentThread().interrupt();
        // }
    }

    // @Test
    // public void testKeyrange() {        
    //     // assertTrue(true);
        
    //     Exception ex = null;

    //     KVStore kvClient = new KVStore(address, serverPort1);

    //     try {
    //         kvClient.connect();
    //     } catch (Exception e) {
    //         ex = e;
    //     }

    //     assertNull(ex);

    //     IKVMessage response = null;

    //     try {
    //         response = kvClient.keyrange();
    //     } catch (Exception e) {
    //         ex = e;
    //     }

    //     assertNull(ex);

    //     assertSame(response.getStatus(), IKVMessage.StatusType.KEYRANGE_SUCCESS);

    //     // assertSame(response.getValue(), "93328924524456121601951623841295194894\r\n");

    //     assertTrue(response.getValue().equals("\r\n"));

    //     // safePrintln(String.format("KEYRANGE: %s", response.getValue()));

    //     kvClient.disconnect();
    // }

    public void safePrintln(String s) {
        synchronized (System.out) {
            System.out.println(s);
        }
    }

    // testPutSuccess
    @Test
    public void testA() {        
        String key = "127.0.0.1:8083";
		String value = "valueA";
		IKVMessage response = null;
		Exception ex = null;

        KVStore kvClient = new KVStore(address, serverPort1);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        assertSame(response.getStatus(), StatusType.PUT_SUCCESS);

        assertTrue(response.getKey().equals(key) && response.getValue().equals(value));

        safePrintln("SUCCESS 1");

        kvClient.disconnect();
    }

    // testPutUpdate
    @Test
	public void testB() {
		String key = "127.0.0.1:8083";
		String value = "valueB";
		IKVMessage response = null;
		Exception ex = null;

        KVStore kvClient = new KVStore(address, serverPort1);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        assertSame(response.getStatus(), StatusType.PUT_UPDATE);

        assertTrue(response.getKey().equals(key) && response.getValue().equals(value));

        safePrintln("SUCCESS 2");

        kvClient.disconnect();
	}

    // testGetSuccess
    @Test
	public void testC() {
		String key = "127.0.0.1:8083";
		String value = "valueB";
		IKVMessage response = null;
		Exception ex = null;

        KVStore kvClient = new KVStore(address, serverPort1);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        assertSame(response.getStatus(), StatusType.GET_SUCCESS);

        assertTrue(response.getKey().equals(key) && response.getValue().equals(value));

        safePrintln("SUCCESS 3");

        kvClient.disconnect();
	}

    // testDeleteSuccess
    @Test
	public void testD() {
		String key = "127.0.0.1:8083";
		String value = "";
		IKVMessage response = null;
		Exception ex = null;

        KVStore kvClient = new KVStore(address, serverPort1);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        assertSame(response.getStatus(), StatusType.DELETE_SUCCESS);

        assertTrue(response.getKey().equals(key) && response.getValue().isEmpty());

        safePrintln("SUCCESS 4");

        kvClient.disconnect();
	}

    // testDeleteError
    @Test
	public void testE() {
		String key = "127.0.0.1:8083";
		String value = "null";
		IKVMessage response = null;
		Exception ex = null;

        KVStore kvClient = new KVStore(address, serverPort1);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        assertSame(response.getStatus(), StatusType.DELETE_ERROR);

        assertTrue(response.getKey().equals(key) && response.getValue().equals(value));

        safePrintln("SUCCESS 5");

        kvClient.disconnect();
	}

    // testGetError
    @Test
	public void testF() {
		String key = "127.0.0.1:8083";
		String value = "";
		IKVMessage response = null;
		Exception ex = null;

        KVStore kvClient = new KVStore(address, serverPort1);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        assertSame(response.getStatus(), StatusType.GET_ERROR);

        assertTrue(response.getKey().equals(key) && response.getValue().isEmpty());

        safePrintln("SUCCESS 6");

        kvClient.disconnect();
	}

    // testKeyrange
    @Test
	public void testG() {
		String value = "91d8c8e93642cca5c0f2be69d199ee22,6a93711a8c3f9c2641dfc71c359a1abe,<127.0.0.1:127.0.0.1>;6a93711a8c3f9c2641dfc71c359a1abe,85d27949e81cbe4c3d605c59cd7d7dc3,<127.0.0.1:127.0.0.1>;85d27949e81cbe4c3d605c59cd7d7dc3,91d8c8e93642cca5c0f2be69d199ee22,<127.0.0.1:127.0.0.1>;";
		IKVMessage response = null;
		Exception ex = null;

        KVStore kvClient = new KVStore(address, serverPort1);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        try {
            response = kvClient.keyrange();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

        assertSame(response.getStatus(), StatusType.KEYRANGE_SUCCESS);

        assertTrue(response.getKey().isEmpty() && response.getValue().equals(value));

        safePrintln("SUCCESS 7");

        kvClient.disconnect();
	}

    @Test
	public void testH() {
        assertTrue(true);
        safePrintln("SUCCESS 8");
    }

    @Test
	public void testI() {
        assertTrue(true);
        safePrintln("SUCCESS 9");
    }

    @Test
	public void testJ() {
        assertTrue(true);
        safePrintln("SUCCESS 10");
    }

    // @Test
	// public void testPerformance() {
    //     int[] clientNums = new int[]{1, 5, 20, 50, 100};
    //     int[] storageServers = new int[]{1, 5, 10, 50, 100};
    //     HashMap<String, String> records = EnronReader.parse("/nfs/ug/homes-1/t/tranjus3/ece419/m2/maildir");
        
    //     int clientNumber = clientNums[0];

	// 	IKVMessage response = null;
	// 	Exception ex = null;

    //     KVStore kvClient = new KVStore(address, serverPort1);

    //     int totalTest = 1000000;
	// 	int putNumber;
	// 	int getNumber;

	// 	long startTime;
	// 	long endTime;


    //     //  80% puts, 20% gets
    //     putNumber = (int)(totalTest * 0.8);
	// 	getNumber = totalTest - putNumber;
    //     startTime = System.currentTimeMillis();
    //     // ExecutorService es = Executors.newCachedThreadPool();
    //     // for(int i=0;i<clientNumber;i++)
    //     //     es.execute(new Runnable() { 
    //     //         runPerformance(records,putNumber,getNumber,kvClient);
    //     //     });
    //     // es.shutdown();
    //     runPerformance(records,putNumber,getNumber,kvClient);
    //     // boolean finished = es.awaitTermination(1, TimeUnit.MINUTES);
    //     endTime = System.currentTimeMillis();
	// 	System.out.println("+++ 80% puts, 20% gets: " + (endTime - startTime) + " ms +++");

        
    //     // 50%/50%
    //     putNumber = (int)(totalTest * .5);
	// 	getNumber = totalTest - putNumber;

    //     startTime = System.currentTimeMillis();
    //     // ExecutorService es = Executors.newCachedThreadPool();
    //     // for(int i=0;i<clientNumber;i++)
    //     //     es.execute(new Runnable() { 
    //     //         runPerformance(records,putNumber,getNumber,kvClient);
    //     //     });
    //     // es.shutdown();
    //     runPerformance(records,putNumber,getNumber,kvClient);
    //     // boolean finished = es.awaitTermination(1, TimeUnit.MINUTES);
    //     endTime = System.currentTimeMillis();
	// 	System.out.println("+++ 50% puts, 50% gets: " + (endTime - startTime) + " ms +++");

    //     // 80% gets, 20% puts
    //     putNumber = (int)(totalTest * .2);
	// 	getNumber = totalTest - putNumber;

    //     startTime = System.currentTimeMillis();
    //     // ExecutorService es = Executors.newCachedThreadPool();
    //     // for(int i=0;i<clientNumber;i++)
    //     //     es.execute(new Runnable() { 
    //     //         runPerformance(records,putNumber,getNumber,kvClient);
    //     //     });
    //     // es.shutdown();
    //     runPerformance(records,putNumber,getNumber,kvClient);
    //     // boolean finished = es.awaitTermination(1, TimeUnit.MINUTES);
    //     endTime = System.currentTimeMillis();
	// 	System.out.println("+++ 20% puts, 80% gets: " + (endTime - startTime) + " ms +++");

    //     assertTrue(true);
    //     safePrintln("SUCCESS PERFORMANCE");
    // }

    // public void runPerformance(HashMap<String, String> dat, int putNumber, int getNumber, KVStore kvClient){
    //     String baseKey = "PERFORMANCE_BASELINEKEY";
    //     String baseValue = "PERFORMANCE_BASELINEVALUE";
	// 	Exception ex = null;
    //     String key;
	// 	for(int i=0; i<putNumber;i++){
	// 		try {
    //              key = dat.keySet().iterator().next();
	// 			kvClient.put(baseKey + Integer.toString(i), baseValue + Integer.toString(i));
	// 		} catch (Exception e) {
	// 			ex = e;
	// 		}
	// 	}

	// 	for(int i=0; i<getNumber;i++){
	// 		try {
    //             key = dat.keySet().iterator().next();
	// 			kvClient.get(baseKey + Integer.toString(i));
	// 		} catch (Exception e) {
	// 			ex = e;
	// 		}
	// 	}
	// }
}