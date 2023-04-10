package testing;


import java.util.ArrayList;

import junit.framework.TestCase;
import app_kvServer.KVServer;
import org.junit.Test;

import storage.KVStorage;

import client.KVStore;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import java.lang.Thread;

public class DistributedKVMultiClient extends TestCase {

    private static String strategy = "foo";
	private static String dataDir = "./data/test";
	private static String dataProps = "distributed_kvserver_test.properties";
    public ArrayList<KVServer> serverList;
    public KVStore kvClient;
    public KVStore kvClientA;
    public KVStore kvClientB;

    @Test
    public void testMultiPutNew() {
		IKVMessage responseA = null;
        IKVMessage responseB = null;
		Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
		TestingUtils.spinUpServers(3,dataDir,dataProps);
        kvClientA = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        kvClientB = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        try {
			kvClientA.connect();
			kvClientB.connect();
		} catch (Exception ignored) {
            System.out.println(ignored);
		}

        try {
            responseA = kvClientA.put("keyA", value);
            responseB = kvClientB.put("keyB", value);

            //System.out.println(response);
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        } catch (Exception e) {
            ex = e;
        }
        
        KVStorage kvstore = new KVStorage("./data", String.format("%s:%d.properties", TestingVars.SERVER_A_ADDRESS,TestingVars.PORT_ARRAY[0]));

        inValue = kvstore.exists("keyA");
        try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        inValue = kvstore.exists("keyB");
        try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}

        kvstore = null;
        kvClientA.disconnect();
        kvClientB.disconnect();
        TestingUtils.spinDownServers();
        AllTests.clearTestData();

		assertTrue(ex == null && responseA.getStatus() == StatusType.PUT_SUCCESS && responseB.getStatus() == StatusType.PUT_SUCCESS && inValue);

    }


    @Test
    public void testMultiPutUpdate() {
		IKVMessage responseA = null;
        IKVMessage responseB = null;
		Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
		TestingUtils.spinUpServers(3,dataDir,dataProps);
        kvClientA = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        kvClientB = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        try {
			kvClientA.connect();
			kvClientB.connect();
		} catch (Exception ignored) {
            System.out.println(ignored);
		}

        try {
            responseA = kvClientA.put("keyA", value);
            responseB = kvClientB.put("keyB", value);

            //System.out.println(response);
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        } catch (Exception e) {
            ex = e;
        }

        try {
            responseA = kvClientA.put("keyA", "aGone");
            responseB = kvClientB.put("keyB", "bGone");

            //System.out.println(response);
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        } catch (Exception e) {
            ex = e;
        }
        
        KVStorage kvstore = new KVStorage("./data", String.format("%s:%d.properties", TestingVars.SERVER_A_ADDRESS,TestingVars.PORT_ARRAY[0]));
        System.out.println(kvstore.getDatabase());
        inValue = false;
        try{
            inValue = kvstore.getDatabase().get("keyA").equals("aGone");
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
            inValue = inValue && kvstore.getDatabase().get("keyB").equals("bGone");
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        } catch (Exception e){}

        kvstore = null;
        kvClientA.disconnect();
        kvClientB.disconnect();
        TestingUtils.spinDownServers();
        AllTests.clearTestData();

		assertTrue(ex == null && responseA.getStatus() == StatusType.PUT_UPDATE && responseB.getStatus() == StatusType.PUT_UPDATE && inValue);

    }

    @Test
    public void testMultiDeleteUpdate() {
		IKVMessage responseA = null;
        IKVMessage responseB = null;
		Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
		TestingUtils.spinUpServers(3,dataDir,dataProps);
        kvClientA = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        kvClientB = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        try {
			kvClientA.connect();
			kvClientB.connect();
		} catch (Exception ignored) {
            System.out.println(ignored);
		}

        try {
            responseA = kvClientA.put("keyA", value);
            responseB = kvClientB.put("keyB", value);
            responseB = kvClientB.put("keyC", value);

            
            //System.out.println(response);
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        } catch (Exception e) {
            ex = e;
        }

        try {
            responseA = kvClientA.put("keyA", "");
            responseB = kvClientB.put("keyB", "");
            

            //System.out.println(response);
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        } catch (Exception e) {
            ex = e;
        }
        
        KVStorage kvstore = new KVStorage("./data", String.format("%s:%d.properties", TestingVars.SERVER_A_ADDRESS,TestingVars.PORT_ARRAY[1]));
        
        inValue = false;
        try{
            inValue = kvstore.getDatabase().get("keyA") == null ;
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
            inValue = inValue && kvstore.getDatabase().get("keyB") == null;
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        } catch (Exception e){}

        kvstore = null;
        kvClientA.disconnect();
        kvClientB.disconnect();
        TestingUtils.spinDownServers();
        AllTests.clearTestData();

		assertTrue(ex == null && responseA.getStatus() == StatusType.DELETE_SUCCESS && responseB.getStatus() == StatusType.DELETE_SUCCESS && inValue);

    }

    @Test
    public void testPutDeleteCollisionRep() {
		IKVMessage responseA = null;
        IKVMessage responseB = null;
		Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
		TestingUtils.spinUpServers(3,dataDir,dataProps);
        kvClientA = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        kvClientB = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        try {
			kvClientA.connect();
			kvClientB.connect();
		} catch (Exception ignored) {
            System.out.println(ignored);
		}

        try {
            responseA = kvClientA.put("keyA", value);

            
            //System.out.println(response);
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        } catch (Exception e) {
            ex = e;
        }

        try {
            responseA = kvClientA.put("keyA", "aabb");
            responseB = kvClientB.put("keyA", "");
            

            //System.out.println(response);
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        } catch (Exception e) {
            ex = e;
        }
        
        KVStorage kvstore = new KVStorage("./data", String.format("%s:%d.properties", TestingVars.SERVER_A_ADDRESS,TestingVars.PORT_ARRAY[1]));
        
        inValue = false;
        try{
            inValue = kvstore.getDatabase().get("keyA") == null ;
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        } catch (Exception e){}

        kvstore = null;
        kvClientA.disconnect();
        kvClientB.disconnect();
        TestingUtils.spinDownServers();
        AllTests.clearTestData();

		assertTrue(ex == null && responseA.getStatus() == StatusType.PUT_UPDATE && responseB.getStatus() == StatusType.DELETE_SUCCESS && inValue);

    }

    @Test
    public void testDeleteReplicaAndPrimary() {
		IKVMessage responseA = null;
        IKVMessage responseB = null;
		Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
		TestingUtils.spinUpServers(3,dataDir,dataProps);
        kvClientA = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        kvClientB = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_B_PORT);
        try {
			kvClientA.connect();
			kvClientB.connect();
		} catch (Exception ignored) {
            System.out.println(ignored);
		}

        try {
            responseA = kvClientA.put("keyA", value);

            
            //System.out.println(response);
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        } catch (Exception e) {
            ex = e;
        }

        try {
            responseA = kvClientA.put("keyA", "");
            responseB = kvClientB.put("keyA", "");
            

            //System.out.println(response);
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        } catch (Exception e) {
            ex = e;
        }
        
        KVStorage kvstore = new KVStorage("./data", String.format("%s:%d.properties", TestingVars.SERVER_A_ADDRESS,TestingVars.PORT_ARRAY[1]));
        
        inValue = false;
        try{
            inValue = kvstore.getDatabase().get("keyA") == null ;
            try{Thread.sleep(500);}catch(InterruptedException e){System.out.println(e);}
        } catch (Exception e){}

        kvstore = null;
        kvClientA.disconnect();
        kvClientB.disconnect();
        TestingUtils.spinDownServers();
        AllTests.clearTestData();

		assertTrue(ex == null && responseA.getStatus() == StatusType.DELETE_SUCCESS && responseB.getStatus() == StatusType.DELETE_ERROR && inValue);

    }
}