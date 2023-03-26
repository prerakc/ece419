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
import java.util.*;


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
            this.sortedHashes = new String[this.numNodes];
            for(int i  =0; i< this.numNodes; i++){
                String key = ip + ":" + i;
                ECSNode node = new ECSNode(key, ip, i);
                this.hr.addServer(node.getIpPortHash(), node);
                String hash = HashUtils.getHashString(key);
                this.sortedHashes[i] = hash;
            }
            Arrays.sort(this.sortedHashes);

            // ECSNode node1 = new ECSNode();
        }

        @Test
        public void testGetNodeFromHash() {   
            String hash = this.sortedHashes[0];
            ECSNode fetchedNode = this.hr.getServerForHashValue(hash);
            assertEquals(hash, fetchedNode.getIpPortHash());
        }

        @Test
        public void testGetSuccessor(){
            ECSNode succ = this.hr.getSuccessorNodeFromIpHash(this.sortedHashes[1]);
            assertEquals(succ.getIpPortHash(), this.sortedHashes[2]);
        }

        @Test 
        public void testGetPredecessor(){
            ECSNode pred = this.hr.getPredecessorNodeFromIpHash(sortedHashes[1]);
            assertEquals(pred.getIpPortHash(), this.sortedHashes[0]);

        }



        

}