package ecs;

import java.util.Map;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import java.math.BigInteger;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import app_kvServer.KVServer;
import shared.messages.IKVMessage.StatusType;
import shared.zookeeper_comms.ZKManagerImpl;
import storage.HashRing;
import java.util.TreeMap;
import java.util.Collections;
import java.util.ArrayList;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.file.WatchEvent;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import java.util.Arrays;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import storage.HashUtils;

public class ECS {
    private static Logger logger = Logger.getRootLogger();

    static List<IECSNode> kvNodes;

    private static ZKManagerImpl zkMng;

    public static final String ZOOKEEPER_SERVER_SHELL = System.getProperty("user.dir") + "/zookeeper-3.4.11/bin/zkServer.sh";
    public static final String ZOOKEEPER_CONF = System.getProperty("user.dir") + "/zookeeper-3.4.11/conf/zoo_sample.cfg";

    private static int calls = 1;
    private ZooKeeper zookeeper;
    private CountDownLatch connected;
    private static String connectString;

    private static HashRing hr;

    public ECS(String address, int port, boolean ecsMaster){
        ECS.connectString = String.format("%s:%d",address,port);
        ECS.kvNodes = Collections.synchronizedList(new ArrayList<IECSNode>());
        hr = new HashRing();
        this.initializeZK();

        if (ecsMaster){
            try {
                ECS.zkMng.getZNodeChildrenCallback("/workers", ECS.serverListWatchCallback(), ECS.getChildrenChangeCallback());
            } catch (InterruptedException | IOException | KeeperException e){
                System.out.println("ERROR");
                logger.error("Unexpected exception", e);
            }
        }
    }

    public void addStatusWatch(String name){
        try {
            ECS.zkMng.getZNodeDataCallback("/server_status/"+name, ECS.serverStatusWatchCallback());
        } catch (InterruptedException | IOException | KeeperException e){
            System.out.println("ERROR");
            logger.error("Unexpected exception", e);
        }
    }

    public void addMetadataWatch(String name){
        try {
            ECS.zkMng.getZNodeDataCallback("/workers/"+name, ECS.workerDataWatchCallback());
        } catch (InterruptedException | IOException | KeeperException e){
            System.out.println("ERROR");
            logger.error("Unexpected exception", e);
        }
    }

    public static void addECSNode(String address, int port){
        ECSNode newNode = new ECSNode(String.format("%s:%d",address,port), address, port,null);

        ECS.hr.addServer(newNode.getIpPortHash(), newNode);
        // logger.info("HR SIZE -- PRE: " + ECS.hr.getHashRing().size());

        //if there is only one node in ring 
        if (ECS.hr.getHashRing().size() == 1){
            System.out.println(newNode.getIpPortHash());
            ECS.hr.getFirstValue().assignHashRange(HashRing.incrementHexString(newNode.getIpPortHash()), newNode.getIpPortHash());
            // ECS.hr.getFirstValue().assignHashRange((new BigInteger(newNode.getIpPortHash(), 16).add(BigInteger.ONE)).toString(16), newNode.getIpPortHash());
        } else {
            //TODO CORNER CASE OF LESS THAN 3 NODES, UPDATE THE STATUS OF NODES DURRING CHANGE
            ECSNode pred = ECS.hr.getPredecessorNodeFromIpHash(newNode.getIpPortHash());
            ECSNode succ = ECS.hr.getSuccessorNodeFromIpHash(newNode.getIpPortHash());

            newNode.assignHashRange(pred.getNodeHashRange()[1], newNode.getNodeHashRange()[1]);
            succ.assignHashRange(newNode.getNodeHashRange()[1], succ.getNodeHashRange()[1]);

            // remove the old version of nodes
            ECS.hr.addServer(newNode.getIpPortHash(), newNode);
            
            ECS.hr.removeServer(pred.getIpPortHash());
            ECS.hr.addServer(pred.getIpPortHash(), pred);
            // if only two servers connected
            if (ECS.hr.getHashRing().size() == 2){
                ECS.hr.removeServer(succ.getIpPortHash());
                ECS.hr.addServer(succ.getIpPortHash(), succ);
            }

        }
        // logger.info("HR SIZE -- POST: " + ECS.hr.getHashRing().size());
    }

    public static void removeECSNode(String address, int port){
        ECSNode removeNode = new ECSNode(String.format("%s:%d",address,port), address, port,null);
        ECS.hr.removeServer(removeNode.getIpPortHash());
        if (ECS.hr.getHashRing().size() == 1){
            System.out.println(removeNode.getIpPortHash());
            // ECS.hr.getFirstValue().assignHashRange(removeNode.getIpPortHash(), ECS.hr.getFirstValue().getNodeHashRange()[1]);
            ECSNode onlyNode = ECS.hr.getFirstValue();
            ECS.hr.getFirstValue().assignHashRange(HashRing.incrementHexString(onlyNode.getIpPortHash()), onlyNode.getIpPortHash());
            // ECS.hr.getFirstValue().assignHashRange((new BigInteger(onlyNode.getIpPortHash(), 16).add(BigInteger.ONE).toString(16)), onlyNode.getIpPortHash());

        } else if (ECS.hr.getHashRing().size() == 0){
            
        } else {
            ECSNode pred = ECS.hr.getPredecessorNodeFromIpHash(removeNode.getIpPortHash());
            ECSNode succ = ECS.hr.getSuccessorNodeFromIpHash(removeNode.getIpPortHash());

            succ.assignHashRange(pred.getNodeHashRange()[1], succ.getNodeHashRange()[1]);


            // remove the old version of nodes
            ECS.hr.addServer(removeNode.getIpPortHash(), removeNode);
                
            ECS.hr.removeServer(pred.getIpPortHash());
            ECS.hr.addServer(pred.getIpPortHash(), pred);

            ECS.hr.addServer(succ.getIpPortHash(), succ);
            ECS.hr.removeServer(succ.getIpPortHash());
        }
    }

    public void initializeZK(){
        this.startZookeeper();
        createZookeeperManager();
        this.createWorkersNode();
        this.createServerNode();
        //this.createMetadataNode();
        //this.createServerNode();
    }

    private void createZookeeperManager(){
        try{
             ECS.zkMng = new ZKManagerImpl(ECS.connectString);
        } catch (IOException | InterruptedException e){
            logger.error("Unable to create Zookeeper Manager", e);
            e.printStackTrace();
        }
    }

    // Creates listener for change in server list
    public static Watcher serverListWatchCallback(){
        return new Watcher() { 
            public void process(WatchedEvent e){
                if(e.getType() == EventType.NodeChildrenChanged) {
                    logger.info(String.format("ZNode children change on %s", e.getPath()));
                    try {
                        ECS.zkMng.getZNodeChildrenCallback("/workers", ECS.serverListWatchCallback(), ECS.getChildrenChangeCallback());
                    } catch (InterruptedException | IOException | KeeperException ex){
                        logger.error("Unexpected exception", ex);
                    }
                }
            }
        };
    }

    private static ChildrenCallback getChildrenChangeCallback(){
        return new ChildrenCallback() {
            public void processResult(int rc, String path, Object ctx,
                                      List<String> children) {
                // logger.info("REACHED CALLBACK FOR CHILDREN CHANGE");
                ECS.updateKeyRange(path,children);
            }
        };
    }

    private static Watcher serverStatusWatchCallback(){
        return new Watcher() {
            public void process(WatchedEvent e){
                if(e.getType() == EventType.NodeDataChanged){
                    logger.info(String.format("Server status change on %s", e.getPath()));
                    try {
                        KVServer.updateStatusZK(ECS.zkMng.getZNodeDataCallback(e.getPath(), ECS.serverStatusWatchCallback()));
                    } catch (InterruptedException | IOException | KeeperException ex){
                        logger.error("Unexpected exception", ex);
                    }
                }
            }
        };
    }

    private static Watcher workerDataWatchCallback(){
        return new Watcher() {
            public void process(WatchedEvent e){
                if(e.getType() == EventType.NodeDataChanged){
                    logger.info(String.format("Server status change on %s", e.getPath()));
                    try {
                        KVServer.updateMetadataZK(ECS.zkMng.getZNodeDataCallback(e.getPath(), ECS.workerDataWatchCallback()));
                    } catch (InterruptedException | IOException | KeeperException ex){
                        logger.error("Unexpected exception", ex);
                    }
                }
            }
        };
    }

    public static synchronized void updateKeyRange(String path,List<String> workerNames){
        System.out.println(path);
        System.out.println(Arrays.toString(workerNames.toArray()));

        String critNode;
        if (workerNames.size() == 0 && ECS.hr.getHashRing().values().size() == 0){
            return;
        }

        List<String> kvCache = new ArrayList<String>();
        for (IECSNode i : ECS.hr.getHashRing().values()) {
            kvCache.add(i.getNodeName());
            // set all existing servers to read only before editing ranges
        }

        List<String> differences = null;
        // System.out.println(Arrays.toString(kvCache.toArray()));
        if (workerNames.size() > kvCache.size()){
            differences = new ArrayList<>(workerNames);
            differences.removeAll(kvCache);
        } else {
            differences = new ArrayList<>(kvCache);
            differences.removeAll(workerNames);
        }
        
        logger.info(workerNames.size());
        logger.info(kvCache.size());
        critNode = differences.get(0);
        


        // freeze node that is to be killed or newly added
        // try{
        //     logger.info("++++++STOPPING NEWLY ADDED REMOVED SERVER+++++");
        //     ECS.zkMng.update("/server_status/"+critNode, Integer.toString(StatusType.SERVER_STOPPED.ordinal()).getBytes());
        // } catch (KeeperException | InterruptedException e){
        //     logger.error("Unable to update server status",e);
        // }
        // if adding a server
        if (workerNames.size() > kvCache.size()){
            // write lock all existing nodes
            for (String name : kvCache) {
                try{
                    ECS.zkMng.update("/server_status/"+name, Integer.toString(StatusType.SERVER_WRITE_LOCK.ordinal()).getBytes());
                } catch (KeeperException | InterruptedException e){
                    logger.error("Unable to update server status",e);
                }
            }
            ECS.addECSNode(critNode.split(":")[0],Integer.parseInt(critNode.split(":")[1]));
            
        } else {
            // write lock all existing nodes
            for (String name : workerNames) {
                try{
                    ECS.zkMng.update("/server_status/"+name, Integer.toString(StatusType.SERVER_WRITE_LOCK.ordinal()).getBytes());
                } catch (KeeperException | InterruptedException e){
                    logger.error("Unable to update server status",e);
                }
            }
            // ir removing a server
            ECS.removeECSNode(critNode.split(":")[0],Integer.parseInt(critNode.split(":")[1]));
        }
        publishMetadata();
        // once all metadata published set all servers to ready
        for (String name : workerNames) {
            try{
                ECS.zkMng.update("/server_status/"+name, Integer.toString(StatusType.SERVER_IDLE.ordinal()).getBytes());
            } catch (KeeperException | InterruptedException e){
                logger.error("Unable to update server status",e);
            }
        }
    }
    
    public static void publishMetadata(){
        StringBuilder sb = new StringBuilder();
        logger.info("PMETADAT HR: " + ECS.hr.getHashRing().keySet());
        TreeMap<String, ECSNode> hrMap = ECS.hr.getHashRing();
        // logger.info("HR SIZE publish: " + ECS.hr.getHashRing().size());
        for (ECSNode i : hrMap.values()) {
            try{
                logger.info("IN HASH RING: " + i.getNodeName());
                sb.append(i.serialize());
            } catch (Exception e){
                logger.error("Unable to serialize node", e);
            }
        }
        for (ECSNode i : hrMap.values()) {
            try {
                // logger.info("/workers/"+i.getNodeName());
                ECS.zkMng.update("/workers/"+i.getNodeName(), sb.toString().getBytes());
            } catch (KeeperException | InterruptedException e){
                System.out.println("ERROR");
                logger.error("Unexpected exception", e);
            }
        }
    }

    public String getNodeData(String path){
        try {
            return ECS.zkMng.getZNodeData("/workers/"+path,false);
        } catch (InterruptedException | IOException | KeeperException e){
            System.out.println("ERROR");
            logger.error("Unexpected exception", e);
        }
        return "";
    }

    private void createWorkersNode() {
        try{
            ECS.zkMng.create("/workers", "test".getBytes());
        } catch (KeeperException | InterruptedException e){
            logger.error("Unable to create znodes", e);
        }
    }

    private void createMetadataNode() {
        try{
            ECS.zkMng.create("/metadata", "test".getBytes());
        } catch (KeeperException | InterruptedException e){
            logger.error("Unable to create znodes", e);
        }
    }
    
    public void addServer(String name){
        try{
            ECS.zkMng.create("/workers/"+name, "".getBytes());
        } catch (KeeperException | InterruptedException e){
            logger.error("Unable to create znodes", e);
        }
    }

    public void removeServer(String name){
        try{
            ECS.zkMng.deleteZNode("/workers/"+name);
        } catch (KeeperException | InterruptedException e){
            logger.error("Unable to create znodes", e);
        }
    }

    public void addServerStatus(String name){
        try{
            ECS.zkMng.create("/server_status/"+name, "".getBytes());
        } catch (KeeperException | InterruptedException e){
            logger.error("Unable to create znodes", e);
        }
    }

    public void removeServerStatus(String name){
        try{
            ECS.zkMng.deleteZNode("/server_status/"+name);
        } catch (KeeperException | InterruptedException e){
            logger.error("Unable to create znodes", e);
        }
    }

    private void createServerNode() {
        try{
            ECS.zkMng.create("/server_status", "".getBytes());
        } catch (KeeperException | InterruptedException e){
            logger.error("Unable to create znodes", e);
        }
    }

        // TODO CHANGE THIS, COPY PASTED
    public void startZookeeper(){
        try {
            ProcessBuilder zookeeperProcessBuilder =
                    new ProcessBuilder(ZOOKEEPER_SERVER_SHELL, "start", ZOOKEEPER_CONF)
                            .inheritIO();
            Process zookeeperProcess = zookeeperProcessBuilder.inheritIO().start();
            zookeeperProcess.waitFor();

            this.connected = new CountDownLatch(1);
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent e) {
                    if (e.getState() == KeeperState.SyncConnected) {
                        connected.countDown();
                    }
                }
            };
            this.zookeeper = new ZooKeeper(ECS.connectString, 300000000, watcher);
            connected.await();
        } catch (Exception e) {
            logger.error("Unable to start Zookeeper", e);
        }
    }

}
