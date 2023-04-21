package testing;

import java.util.ArrayList;

import junit.framework.TestCase;
import app_kvServer.KVServer;
import org.junit.Test;

import storage.KVStorage;

import client.KVStore;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;
import java.net.*;

import app_kvClient.TempServerConnection;
import app_kvClient.NotificationServer;
import java.lang.Thread;

import java.io.*;

public class DistributedKVMultiClient extends TestCase {

    private static String strategy = "foo";
    private static String dataDir = "./data/test";
    private static String dataProps = "distributed_kvserver_test.properties";
    public ArrayList<KVServer> serverList;
    public KVStore kvClient;
    public KVStore kvClientA;
    public KVStore kvClientB;

    @Test 
    public void testSubscribe(){
        IKVMessage responseA = null;
        Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
        int clientPort = 8086;
        

        TestingUtils.spinUpServers(3, dataDir, dataProps);
        kvClientA = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);

        try {
            kvClientA.connect();
        } catch (Exception ignored) {
            System.out.println(ignored);
        }
        try{
            responseA = kvClientA.notify2(IKVMessage.StatusType.NOTIFICATION_TOGGLE,
                            String.format("%s:%d", "127.0.0.1", clientPort), "subscribe");
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        TestingUtils.spinDownServers();
        AllTests.clearTestData();

        assertTrue(ex == null && responseA.getStatus() == StatusType.NOTIFICATION_ACK
                && responseA.getKey().equals(String.format("%s:%d", "127.0.0.1", clientPort)) && responseA.getValue().equals("subscribe"));
    }

    @Test 
    public void testSubThenUnsub(){
        IKVMessage responseA = null;
        IKVMessage responseB = null;

        Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
        int clientPort = 8086;
        

        TestingUtils.spinUpServers(3, dataDir, dataProps);
        kvClientA = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);

        try {
            kvClientA.connect();
        } catch (Exception ignored) {
            System.out.println(ignored);
        }

        try{
            responseA = kvClientA.notify2(IKVMessage.StatusType.NOTIFICATION_TOGGLE,
                            String.format("%s:%d", "127.0.0.1", clientPort), "subscribe");
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        try{
            responseB = kvClientA.notify2(IKVMessage.StatusType.NOTIFICATION_TOGGLE,
                            String.format("%s:%d", "127.0.0.1", clientPort), "unsubscribe");
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        TestingUtils.spinDownServers();
        AllTests.clearTestData();

        assertTrue(ex == null && responseA.getStatus() == StatusType.NOTIFICATION_ACK
                && responseA.getKey().equals(String.format("%s:%d", "127.0.0.1", clientPort)) && responseA.getValue().equals("subscribe")
                && responseB.getStatus() == StatusType.NOTIFICATION_ACK
                && responseB.getKey().equals(String.format("%s:%d", "127.0.0.1", clientPort)) && responseB.getValue().equals("unsubscribe"));
    }

    @Test 
    public void testSubPut(){
        IKVMessage responseA = null;
        IKVMessage responseB = null;

        Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
        int clientPort = 8089;

        TestingUtils.spinUpServers(3, dataDir, dataProps);
        kvClientA = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);

        try {
            kvClientA.connect();
        } catch (Exception ignored) {
            System.out.println(ignored);
        }



        // start notification server
        try {
            new NotificationServer(clientPort).start();
        } catch (Exception e) {
			ex=e;
		}

        // capture output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // old stream
        PrintStream old = System.out;
        // set new stream
        System.setOut(new PrintStream(baos));

        try{
            responseA = kvClientA.notify2(IKVMessage.StatusType.NOTIFICATION_TOGGLE,
                            String.format("%s:%d", "127.0.0.1", clientPort), "subscribe");
            kvClientA.put("keyA", value);           
        } catch (Exception e) {
            ex = e;
        }
        System.out.flush();
        System.setOut(old);
        // System.out.println("LOOKING HERE "+ baos.toString().split("\n")[2] + "AND HERE");
        TestingUtils.spinDownServers();
        AllTests.clearTestData();

        assertTrue(ex == null && responseA.getStatus() == StatusType.NOTIFICATION_ACK
                && responseA.getKey().equals(String.format("%s:%d", "127.0.0.1", clientPort)) && responseA.getValue().equals("subscribe")
                && baos.toString().split("\n")[2].equals("Notification> Value of key 'keyA' has been changed to 'test'"));
    }

    @Test 
    public void testSubDel(){
        IKVMessage responseA = null;
        IKVMessage responseB = null;

        Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
        int clientPort = 8090;

        TestingUtils.spinUpServers(3, dataDir, dataProps);
        kvClientA = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_B_PORT);

        try {
            kvClientA.connect();
            kvClientA.put("keyA", value);           
        } catch (Exception ignored) {
            System.out.println(ignored);
        }



        // start notification server
        try {
            new NotificationServer(clientPort).start();
        } catch (Exception e) {
           // e.printStackTrace();
			ex=e;
		}

        // capture output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // old stream
        PrintStream old = System.out;
        // set new stream
        System.setOut(new PrintStream(baos));

        try{
            responseA = kvClientA.notify2(IKVMessage.StatusType.NOTIFICATION_TOGGLE,
                            String.format("%s:%d", "127.0.0.1", clientPort), "subscribe");
            kvClientA.put("keyA", "null");
            Thread.sleep(100);           
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }
        
        System.out.flush();
        System.setOut(old);
        System.out.println(baos.toString());
        TestingUtils.spinDownServers();
        AllTests.clearTestData();

        //System.out.println("TESTSUBDEL      " + baos.toString());

        assertTrue(ex == null && responseA.getStatus() == StatusType.NOTIFICATION_ACK
                && responseA.getKey().equals(String.format("%s:%d", "127.0.0.1", clientPort)) && responseA.getValue().equals("subscribe")
                && baos.toString().split("\n")[0].equals("Notification> Key 'keyA' has been deleted"));
    }

    @Test 
    public void testSubUpdate(){
        IKVMessage responseA = null;
        IKVMessage responseB = null;

        Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
        int clientPort = 8089;

        TestingUtils.spinUpServers(3, dataDir, dataProps);
        kvClientA = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);

        try {
            kvClientA.connect();
            kvClientA.put("keyA", value);           
        } catch (Exception ignored) {
            System.out.println(ignored);
        }



        // start notification server
        try {
            new NotificationServer(clientPort).start();
        } catch (Exception e) {
			ex=e;
		}

        // capture output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // old stream
        PrintStream old = System.out;
        // set new stream
        System.setOut(new PrintStream(baos));

        try{
            responseA = kvClientA.notify2(IKVMessage.StatusType.NOTIFICATION_TOGGLE,
                            String.format("%s:%d", "127.0.0.1", clientPort), "subscribe");
            kvClientA.put("keyA", "update");           
        } catch (Exception e) {
            ex = e;
        }
        System.out.flush();
        System.setOut(old);
        // System.out.println(baos.toString().split("\n")[0]);
        TestingUtils.spinDownServers();
        AllTests.clearTestData();

        assertTrue(ex == null && responseA.getStatus() == StatusType.NOTIFICATION_ACK
                && responseA.getKey().equals(String.format("%s:%d", "127.0.0.1", clientPort)) && responseA.getValue().equals("subscribe")
                && baos.toString().split("\n")[0].equals("Notification> Value of key 'keyA' has been changed to 'update'"));
    }

    @Test 
    public void testMultiSubPut(){
        IKVMessage responseA = null;
        IKVMessage responseB = null;

        Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
        int clientPortA = 8089;
        int clientPortB = 8090;


        TestingUtils.spinUpServers(3, dataDir, dataProps);
        kvClientA = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);

        try {
            kvClientA.connect();
        } catch (Exception ignored) {
            System.out.println(ignored);
        }



        // start notification server
        try {
            new NotificationServer(clientPortA).start();
            new NotificationServer(clientPortB).start();
        } catch (Exception e) {
			ex=e;
		}

        // capture output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // old stream
        PrintStream old = System.out;
        // set new stream
        System.setOut(new PrintStream(baos));

        try{
            responseA = kvClientA.notify2(IKVMessage.StatusType.NOTIFICATION_TOGGLE,
                            String.format("%s:%d", "127.0.0.1", clientPortA), "subscribe");
            responseB = kvClientA.notify2(IKVMessage.StatusType.NOTIFICATION_TOGGLE,
                            String.format("%s:%d", "127.0.0.1", clientPortB), "subscribe");
            kvClientA.put("keyA", value);           
        } catch (Exception e) {
            ex = e;
        }
        System.out.flush();
        System.setOut(old);
        System.out.println("LOOK HERE");

        System.out.println(baos.toString());
    
        System.out.println(baos.toString().split("\n")[2]);
        System.out.println(baos.toString().split("\n")[3]);
        System.out.println("LOOK HERE");

        TestingUtils.spinDownServers();
        AllTests.clearTestData();

        assertTrue(ex == null && responseA.getStatus() == StatusType.NOTIFICATION_ACK
                && responseA.getKey().equals(String.format("%s:%d", "127.0.0.1", clientPortA)) && responseA.getValue().equals("subscribe")
                && responseB.getStatus() == StatusType.NOTIFICATION_ACK
                && responseB.getKey().equals(String.format("%s:%d", "127.0.0.1", clientPortB)) && responseB.getValue().equals("subscribe")
                && baos.toString().split("\n")[2].equals(baos.toString().split("\n")[3]) 
                && baos.toString().split("\n")[2].equals("Notification> Value of key 'keyA' has been changed to 'test'")
                );
    }

    @Test 
    public void testMultiSubDelete(){
        IKVMessage responseA = null;
        IKVMessage responseB = null;

        Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
        int clientPortA = 8089;
        int clientPortB = 8090;


        TestingUtils.spinUpServers(3, dataDir, dataProps);
        kvClientA = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);

        try {
            kvClientA.connect();
            kvClientA.put("keyA", "test");   
        } catch (Exception ignored) {
            System.out.println(ignored);
        }



        // start notification server
        try {
            new NotificationServer(clientPortA).start();
            new NotificationServer(clientPortB).start();
        } catch (Exception e) {
			ex=e;
		}

        // capture output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // old stream
        PrintStream old = System.out;
        // set new stream
        System.setOut(new PrintStream(baos));

        try{
            responseA = kvClientA.notify2(IKVMessage.StatusType.NOTIFICATION_TOGGLE,
                            String.format("%s:%d", "127.0.0.1", clientPortA), "subscribe");
            responseB = kvClientA.notify2(IKVMessage.StatusType.NOTIFICATION_TOGGLE,
                            String.format("%s:%d", "127.0.0.1", clientPortB), "subscribe");
            kvClientA.put("keyA", "");           
        } catch (Exception e) {
            ex = e;
        }
        System.out.flush();
        System.setOut(old);
        System.out.println("LOOK HERE1");

        System.out.println(baos.toString());
    
        System.out.println(baos.toString().split("\n")[0]);
        System.out.println(baos.toString().split("\n")[1]);
        System.out.println("LOOK HERE1");

        TestingUtils.spinDownServers();
        AllTests.clearTestData();

        assertTrue(ex == null && responseA.getStatus() == StatusType.NOTIFICATION_ACK
                && responseA.getKey().equals(String.format("%s:%d", "127.0.0.1", clientPortA)) && responseA.getValue().equals("subscribe")
                && responseB.getStatus() == StatusType.NOTIFICATION_ACK
                && responseB.getKey().equals(String.format("%s:%d", "127.0.0.1", clientPortB)) && responseB.getValue().equals("subscribe")
                && baos.toString().split("\n")[0].equals(baos.toString().split("\n")[1]) 
                && baos.toString().split("\n")[1].equals("Notification> Key 'keyA' has been deleted")
                );
    }

    @Test 
    public void testMultiSubUpdate(){
        IKVMessage responseA = null;
        IKVMessage responseB = null;

        Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
        int clientPortA = 8089;
        int clientPortB = 8090;


        TestingUtils.spinUpServers(3, dataDir, dataProps);
        kvClientA = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);

        try {
            kvClientA.connect();
        } catch (Exception ignored) {
            System.out.println(ignored);
        }



        // start notification server
        try {
            new NotificationServer(clientPortA).start();
            new NotificationServer(clientPortB).start();
        } catch (Exception e) {
			ex=e;
		}

        // capture output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // old stream
        PrintStream old = System.out;
        // set new stream
        System.setOut(new PrintStream(baos));

        try{
            responseA = kvClientA.notify2(IKVMessage.StatusType.NOTIFICATION_TOGGLE,
                            String.format("%s:%d", "127.0.0.1", clientPortA), "subscribe");
            responseB = kvClientA.notify2(IKVMessage.StatusType.NOTIFICATION_TOGGLE,
                            String.format("%s:%d", "127.0.0.1", clientPortB), "subscribe");
            kvClientA.put("keyA", "update");           
        } catch (Exception e) {
            ex = e;
        }
        System.out.flush();
        System.setOut(old);
        System.out.println("LOOK HERE2");

        System.out.println(baos.toString());
    
        System.out.println(baos.toString().split("\n")[2]);
        System.out.println(baos.toString().split("\n")[3]);
        System.out.println("LOOK HERE2");

        TestingUtils.spinDownServers();
        AllTests.clearTestData();

        assertTrue(ex == null && responseA.getStatus() == StatusType.NOTIFICATION_ACK
                && responseA.getKey().equals(String.format("%s:%d", "127.0.0.1", clientPortA)) && responseA.getValue().equals("subscribe")
                && responseB.getStatus() == StatusType.NOTIFICATION_ACK
                && responseB.getKey().equals(String.format("%s:%d", "127.0.0.1", clientPortB)) && responseB.getValue().equals("subscribe")
                && baos.toString().split("\n")[2].equals(baos.toString().split("\n")[3]) 
                && baos.toString().split("\n")[2].equals("Notification> Value of key 'keyA' has been changed to 'update'")
                );
    }

}