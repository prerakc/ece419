package testing;

import java.util.ArrayList;

import junit.framework.TestCase;
import app_kvServer.KVServer;
import org.junit.Test;

import client.KVStore;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import java.io.*;
import java.lang.*;
import java.util.*;

public class TestingUtils {
    public TestingUtils() {
    }

    public static ArrayList<Process> serverInstances = new ArrayList<Process>();

    public static KVStore connect(String addr, int port) {
        KVStore kvClient = new KVStore(addr, port);
        try {
            kvClient.connect();
        } catch (Exception ignored) {
        }
        return kvClient;
    }

    public static void spinUpServers(int count, String dataDir, String dataProps) {
        int size = TestingUtils.serverInstances.size();

        for (int i = size; i < count + size; i++) {
            // System.out.println(i);
            try {
                // System.out.println("java "+ "-jar "+ "m2-server.jar "+
                // TestingVars.SERVER_A_ADDRESS+ " "+TestingVars.PORT_ARRAY[i]+ " "+0+ " foo
                // "+TestingVars.ECS_ADDRESS + " "+TestingVars.ECS_PORT);
                ProcessBuilder builder = new ProcessBuilder("java", "-jar", "m2-server.jar",
                        TestingVars.SERVER_A_ADDRESS, "" + (TestingVars.SERVER_A_PORT + i), "0", "foo",
                        TestingVars.ECS_ADDRESS, "" + TestingVars.ECS_PORT);
                TestingUtils.serverInstances.add(builder.start());
            } catch (Exception e) {
                System.out.println(e);
            }

            try {
                Thread.sleep(750);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }
        // try{Thread.sleep(1000);}catch(InterruptedException e){System.out.println(e);}
    }

    public static void spinDownServers() {
        // System.out.println(TestingUtils.serverInstances.size());
        int size = TestingUtils.serverInstances.size();
        for (Process p : TestingUtils.serverInstances) {
            p.destroy();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }
        TestingUtils.serverInstances = new ArrayList<Process>();
    }

    public static void spinDownServer() {
        TestingUtils.serverInstances.remove(0).destroy();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }

}