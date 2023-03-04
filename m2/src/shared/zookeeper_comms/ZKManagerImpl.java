package shared.zookeeper_comms;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.CreateMode;

public class ZKManagerImpl implements ZKManager {
    private static ZooKeeper zkeeper;
    private static ZKConnection zkConnection;

    public ZKManagerImpl() throws InterruptedException, IOException{
        initialize();
    }

    private void initialize() throws InterruptedException, IOException{
        zkConnection = new ZKConnection();
        zkeeper = zkConnection.connect("localhost");
    }

    public void closeConnection() throws InterruptedException{
        zkConnection.close();
    }

    public void create(String path, byte[] data) 
      throws KeeperException, 
      InterruptedException {
 
        zkeeper.create(
          path, 
          data, 
          ZooDefs.Ids.OPEN_ACL_UNSAFE, 
          CreateMode.PERSISTENT);
    }

    public String getZNodeDataCallback(String path, Watcher callbackWatcher) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        byte[] b = null;
        b = zkeeper.getData(path, callbackWatcher,null);
        return new String(b, "UTF-8");
    }

    public String getZNodeData(String path, boolean watch) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        byte[] b = null;
        b = zkeeper.getData(path, watch, null);
        return new String(b, "UTF-8");
    }

    public void update(String path, byte[] data) throws KeeperException, 
      InterruptedException {
        int version = zkeeper.exists(path, true).getVersion();
        zkeeper.setData(path, data, version);
    }
}