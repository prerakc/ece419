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

public class DistributedKVServerTest extends TestCase {

    private static String strategy = "foo";
    private static String dataDir = "./data/test";
    private static String dataProps = "distributed_kvserver_test.properties";
    public ArrayList<KVServer> serverList;
    public KVStore kvClient;

    @Test
    public void testTotalReplication() {
        IKVMessage response = null;
        Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
        TestingUtils.spinUpServers(3, dataDir, dataProps);
        kvClient = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        try {
            kvClient.connect();
        } catch (Exception ignored) {
            System.out.println(ignored);
        }

        for (int i = 0; i < 5; i++) {
            try {
                response = kvClient.put("key" + i, value);
                // System.out.println(response);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
            } catch (Exception e) {
                ex = e;
            }
        }
        KVStorage kvstore;
        for (int i = 0; i < 3; i++) {
            kvstore = new KVStorage("./data",
                    String.format("%s:%d.properties", TestingVars.SERVER_A_ADDRESS, TestingVars.PORT_ARRAY[i]));
            for (int j = 0; j < 5; j++) {
                inValue = kvstore.exists("key" + j);
                // System.out.println(inValue);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
            }
        }
        kvstore = null;
        kvClient.disconnect();
        TestingUtils.spinDownServers();
        AllTests.clearTestData();

        assertTrue(ex == null && inValue);

        // TestingUtils.spinDownServers();
    }

    @Test
    public void testRemoveReplicas() {
        IKVMessage response = null;
        Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
        TestingUtils.spinUpServers(4, dataDir, dataProps);
        kvClient = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        try {
            kvClient.connect();
        } catch (Exception ignored) {
            System.out.println(ignored);
        }

        for (int i = 0; i < 5; i++) {
            try {
                response = kvClient.put("key" + i, value);
                // System.out.println(response);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
            } catch (Exception e) {
                ex = e;
            }
        }
        kvClient.disconnect();
        TestingUtils.spinDownServer();

        KVStorage kvstore;
        for (int i = 0; i < 3; i++) {
            kvstore = new KVStorage("./data",
                    String.format("%s:%d.properties", TestingVars.SERVER_A_ADDRESS, TestingVars.PORT_ARRAY[i + 1]));
            for (int j = 0; j < 5; j++) {
                inValue = kvstore.exists("key" + j);
                // System.out.println(inValue);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
            }
        }
        kvstore = null;
        TestingUtils.spinDownServers();
        AllTests.clearTestData();

        assertTrue(ex == null && inValue);

        // TestingUtils.spinDownServers();
    }

    @Test
    public void testRestore() {
        IKVMessage response = null;
        Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one node and add some data
        TestingUtils.spinUpServers(3, dataDir, dataProps);
        kvClient = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        try {
            kvClient.connect();
        } catch (Exception ignored) {
            System.out.println(ignored);
        }

        for (int i = 0; i < 5; i++) {
            try {
                response = kvClient.put("key" + i, value);
                // System.out.println(response);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
            } catch (Exception e) {
                ex = e;
            }
        }
        kvClient.disconnect();
        TestingUtils.spinDownServers();
        TestingUtils.spinUpServers(3, dataDir, dataProps);
        KVStorage kvstore;

        kvstore = new KVStorage("./data",
                String.format("%s:%d.properties", TestingVars.SERVER_A_ADDRESS, TestingVars.PORT_ARRAY[1]));
        for (int j = 0; j < 5; j++) {
            inValue = kvstore.exists("key" + j);
            System.out.println(inValue);

            // System.out.println(inValue);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }

        kvstore = null;
        TestingUtils.spinDownServers();
        AllTests.clearTestData();

        assertTrue(ex == null && inValue);

        // TestingUtils.spinDownServers();
    }

    @Test
    public void testStartUpPartialRebalanceFour() {
        IKVMessage response = null;
        Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one 3 and add some data
        TestingUtils.spinUpServers(3, dataDir, dataProps);
        kvClient = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        try {
            kvClient.connect();
        } catch (Exception ignored) {
            System.out.println(ignored);
        }
        // location of all keys primary server
        // 8083 8085 8084 8083 8085
        for (int i = 0; i < 5; i++) {
            try {
                response = kvClient.put("key" + i, value);
                // System.out.println(response);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
            } catch (Exception e) {
                ex = e;
            }
        }
        // spin up 4 node
        TestingUtils.spinUpServers(1, dataDir, dataProps);
        KVStorage kvstore = new KVStorage("./data",
                String.format("%s:%d.properties", TestingVars.SERVER_A_ADDRESS, TestingVars.PORT_ARRAY[0]));

        inValue = kvstore.exists("key2");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
        inValue = kvstore.exists("key0");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
        inValue = kvstore.exists("key3");

        kvstore = null;
        kvClient.disconnect();
        TestingUtils.spinDownServers();
        AllTests.clearTestData();

        assertTrue(ex == null && inValue);

        // TestingUtils.spinDownServers();
    }

    @Test
    public void testShutDownPartialRebalanceFour() {
        IKVMessage response = null;
        Exception ex = null;
        String value = "test";
        boolean inValue = true;
        // start one 3 and add some data
        TestingUtils.spinUpServers(5, dataDir, dataProps);
        kvClient = new KVStore(TestingVars.SERVER_A_ADDRESS, TestingVars.SERVER_A_PORT);
        try {
            kvClient.connect();
        } catch (Exception ignored) {
            System.out.println(ignored);
        }
        // location of all keys primary server
        // 8083 8085 8084 8083 8085
        for (int i = 0; i < 5; i++) {
            try {
                response = kvClient.put("key" + i, value);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
            } catch (Exception e) {
                ex = e;
            }
        }
        // check for proper rebalance
        kvClient.disconnect();
        TestingUtils.spinDownServer();
        KVStorage kvstoreA = new KVStorage("./data",
                String.format("%s:%d.properties", TestingVars.SERVER_A_ADDRESS, TestingVars.PORT_ARRAY[1]));
        inValue = kvstoreA.exists("key1");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
        inValue = kvstoreA.exists("key0");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
        inValue = kvstoreA.exists("key3");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
        inValue = kvstoreA.exists("key4");

        KVStorage kvstoreB = new KVStorage("./data",
                String.format("%s:%d.properties", TestingVars.SERVER_A_ADDRESS, TestingVars.PORT_ARRAY[4]));
        inValue = kvstoreA.exists("key2");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
        inValue = kvstoreA.exists("key0");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
        inValue = kvstoreA.exists("key3");

        kvstoreA = null;
        kvstoreB = null;
        TestingUtils.spinDownServers();
        AllTests.clearTestData();

        assertTrue(ex == null && inValue);

        // TestingUtils.spinDownServers();
    }
}