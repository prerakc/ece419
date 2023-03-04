package ecs;

import java.util.Map;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.zookeeper_comms.ZKManagerImpl;

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

    private static ZKManagerImpl zkMng;

    public static final String ZOOKEEPER_SERVER_SHELL = System.getProperty("user.dir") + "/zookeeper-3.4.11/bin/zkServer.sh";
    public static final String ZOOKEEPER_CONF = System.getProperty("user.dir") + "/zookeeper-3.4.11/conf/zoo_sample.cfg";

    private static int calls = 1;
    private ZooKeeper zookeeper;
    private CountDownLatch connected;
    
    public ECS(){
        this.initializeZK();

        try {
            String test = zkMng.getZNodeDataCallback("/server_status", ECS.serverListWatchCallback());
        } catch (InterruptedException | IOException | KeeperException e){
            System.out.println("ERROR");
            logger.error("Unexpected exception", e);
        }
    }

    public void initializeZK(){
        this.startZookeeper();
        createZookeeperManager();
        this.createMetadataNode();
        this.createServerNode();
    }

    private void createZookeeperManager(){
        try{
             ECS.zkMng = new ZKManagerImpl();
        } catch (IOException | InterruptedException e){
            logger.error("Unable to create Zookeeper Manager", e);
            e.printStackTrace();
        }
    }

    // Creates listener for change in server list
    public static Watcher serverListWatchCallback(){
        return new Watcher() { 
            public void process(WatchedEvent e){
                if(e.getType() == EventType.NodeDataChanged) {
                    logger.info(String.format("ZNode change on %s", e.getPath()));
                    try {
                        zkMng.getZNodeDataCallback("/server_status", ECS.serverListWatchCallback());
                    } catch (InterruptedException | IOException | KeeperException ex){
                        logger.error("Unexpected exception", ex);
                    }
                }
            }
        };
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

    private void createMetadataNode() {
        try{
            ECS.zkMng.create("/metadata", "test".getBytes());
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
            this.zookeeper = new ZooKeeper("localhost", 300000000, watcher);
            connected.await();
        } catch (Exception e) {
            logger.error("Unable to start Zookeeper", e);
        }
    }

}
