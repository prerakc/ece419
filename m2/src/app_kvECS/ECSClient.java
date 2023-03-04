package app_kvECS;

import java.util.Map;
import java.io.IOException;
import java.util.Collection;
import java.util.List;


import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ecs.IECSNode;
import shared.zookeeper_comms.ZKManagerImpl;
import ecs.ECS;

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

public class ECSClient implements IECSClient {
    private static Logger logger = Logger.getRootLogger();

    List<IECSNode> kvNodes;
    private ECS ecs;
    
    
    public ECSClient(){
        this.ecs = new ECS();
    }

    @Override
    public boolean start() {
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/ECS.log", Level.INFO);
            new ECSClient();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
        while(true){}
    }

}
