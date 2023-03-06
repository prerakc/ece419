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
// import shared.Config;
// import storage.HashUtils;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DistributedKVStoreTest extends TestCase {

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
    public void test0() {        
        String key = "127.0.0.1:8083";
		String value = "fuck this course";
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

        safePrintln("SUCCESS 0");

        kvClient.disconnect();
    }

    // testPutUpdate
    @Test
	public void test1() {
		String key = "127.0.0.1:8083";
		String value = "dont come to school tomorrow arno";
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

        safePrintln("SUCCESS 1");

        kvClient.disconnect();
	}

    // testGetSuccess
    @Test
	public void test2() {
		String key = "127.0.0.1:8083";
		String value = "dont come to school tomorrow arno";
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

        safePrintln("SUCCESS 2");

        kvClient.disconnect();
	}

    // testDeleteSuccess
    @Test
	public void test3() {
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

        safePrintln("SUCCESS 3");

        kvClient.disconnect();
	}

    // testDeleteError
    @Test
	public void test4() {
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

        safePrintln("SUCCESS 4");

        kvClient.disconnect();
	}

    // testGetError
    @Test
	public void test5() {
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

        safePrintln("SUCCESS 5");

        kvClient.disconnect();
	}

    // testKeyrange
    @Test
	public void test6() {
		String value = "193863670469131866551315920861471043106,141663729207027941295346580002986138302,<127.0.0.1:127.0.0.1>;141663729207027941295346580002986138302,177880165806739301149123881962940759491,<127.0.0.1:127.0.0.1>;177880165806739301149123881962940759491,193863670469131866551315920861471043106,<127.0.0.1:127.0.0.1>;";
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

        safePrintln("SUCCESS 6");

        kvClient.disconnect();
	}
}