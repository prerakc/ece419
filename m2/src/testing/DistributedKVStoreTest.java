package testing;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Before;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

public class DistributedKVStoreTest extends TestCase {

    private static String address = "127.0.0.1";
    private static int ecsPort = 2181;
    private static int serverPort1 = 60000;
    private static int serverPort2 = 60001;
    private static int cacheSize = 0;
    private static String strategy = "foo";
    private static String dataDir = "./data/test";
    private static String dataProps1 = "distributed_kvstore_test_1.properties";
    private static String dataProps2 = "distributed_kvstore_test_2.properties";

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
        new KVServer(address, serverPort1, cacheSize, strategy, address, ecsPort, dataDir, dataProps1).start();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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

    @Test
    public void testPut() {        
        String key = "foo";
		String value = "bar";
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

        assertTrue(response.getKey().equals("foo") && response.getValue().equals("bar"));

        safePrintln("SUCCESS 1");

        kvClient.disconnect();
    }

    @Test
	public void testPutDisconnected() {
		Exception ex = null;
        
        KVStore kvClient = new KVStore(address, serverPort1);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        
        kvClient.disconnect();

		String key = "foo";
		String value = "bar";
		
		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);

        safePrintln("SUCCESS 2");
	}

    

    public void safePrintln(String s) {
        synchronized (System.out) {
          System.out.println(s);
        }
      }
      

}
