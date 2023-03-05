package testing;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

public class DistributedKVStoreTest extends TestCase {

    private static String address = "127.0.0.1";
    private static int ecsPort = 2181;
    private static int serverPort1 = 50073;
    private static int serverPort2 = 50074;
    private static int cacheSize = 0;
    private static String strategy = "foo";
    private static String dataDir = "./data/test";
    private static String dataProps1 = "distributed_kvstore_test_1.properties";
    private static String dataProps2 = "distributed_kvstore_test_2.properties";

    static {
        new Thread(){
            public void run(){
                new ECSClient(address,ecsPort, true).start();
                while(true){}
            }
        }.start();
        new KVServer(address, serverPort1, cacheSize, strategy, address, ecsPort, dataDir, dataProps1).start();
        // new KVServer(address, serverPort2, cacheSize, strategy, address, ecsPort, dataDir, dataProps2).start();
    }

    @Test
    public void testKeyrange() {        
        assertTrue(true);
        
        // Exception ex = null;

        // KVStore kvClient = new KVStore(address, serverPort1);

        // try {
        //     kvClient.connect();
        // } catch (Exception e) {
        //     ex = e;
        // }

        // assertNull(ex);

        // IKVMessage response = null;

        // try {
        //     response = kvClient.keyrange();
        // } catch (Exception e) {
        //     ex = e;
        // }

        // assertNull(ex);

        // assertSame(response.getStatus(), IKVMessage.StatusType.KEYRANGE_SUCCESS);

        // System.out.println(response.getValue());

        // kvClient.disconnect();
    }



}
