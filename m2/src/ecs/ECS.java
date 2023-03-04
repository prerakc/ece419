package ecs;

import java.util.Map;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.zookeeper_comms.ZKManagerImpl;
import storage.HashRing;

import java.util.Collections;
import java.util.ArrayList;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.Watcher.Event.KeeperState;

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
        this.initializeZK();

        if (ecsMaster){
            try {
                zkMng.getZNodeChildrenCallback("/workers", ECS.serverListWatchCallback(), ECS.getChildrenChangeCallback());
            } catch (InterruptedException | IOException | KeeperException e){
                System.out.println("ERROR");
                logger.error("Unexpected exception", e);
            }
        }
    }

    public static void addECSNode(String address, int port){
        ECSNode newNode = new ECSNode(ECS.connectString, address, port);
        kvNodes.add(newNode);
        hr.addServer("xxx", newNode);
    }

    public static void removeECSNode(String name){
        for (int i=0;i<ECS.kvNodes.size();i++) {
            if (ECS.kvNodes.get(i).getNodeName() == name){
                ECS.kvNodes.remove(i);
            }
        }
        hr.removeServer("XXX");
    }

    public void initializeZK(){
        this.startZookeeper();
        createZookeeperManager();
        this.createWorkersNode();
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
                        zkMng.getZNodeChildrenCallback("/workers", ECS.serverListWatchCallback(), ECS.getChildrenChangeCallback());
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
                logger.info("REACHED CALLBACK FOR CHILDREN CHANGE");
                ECS.updateKeyRange(children);
            }
        };
    }

    public static synchronized void updateKeyRange(List<String> workerNames){
        
        String critNode;
        List<String> kvCache = new ArrayList<String>();
        for (IECSNode i : ECS.kvNodes) {
            kvCache.add(i.getNodeName());
        }

        List<String> differences = new ArrayList<>(kvCache);
        differences.removeAll(workerNames);
        critNode = differences.get(0);

        // if adding a server
        if (workerNames.size() > ECS.kvNodes.size()){
            ECS.addECSNode(critNode,00);
            
        } else {
            // ir removing a server
            removeECSNode(critNode);
        }
    }
    
    public static void calculateKeyRanges(){

    }

    public String getNodeData(String path){
        try {
            return ECS.zkMng.getZNodeData("/server_status",false);
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

    private void createServerNode() {
        try{
            ECS.zkMng.create("/server_status", "test".getBytes());
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
