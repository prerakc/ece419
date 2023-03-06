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
import storage.HashRing;
import storage.HashUtils;
import ecs.ECSNode;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HashRingTests extends TestCase {
        private HashRing hr;
        private String ip = "127.0.0.1";
        private int numNodes = 3;
        private String[] sortedHashes;

        @Before
        public void setUp() {
            this.hr = new HashRing();
            // String[] ports = new String[numNodes];
            String[] sortedHashes = new String[numNodes];
            for(int i  =0; i< numNodes; i++){
                String key = ip + ":" + i;
                ECSNode node = new ECSNode(key, host, i);
                String hash = HashUtils.getHashString(key);
                sortedHashes[i] = hash;
            }
            Arrays.sort(sortedHashes);


            // ECSNode node1 = new ECSNode();
        }

        

}