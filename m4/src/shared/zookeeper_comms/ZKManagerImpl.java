package shared.zookeeper_comms;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import java.util.List;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;

public class ZKManagerImpl implements ZKManager {
    private static ZooKeeper zkeeper;
    private static ZKConnection zkConnection;

    public ZKManagerImpl(String connectString) throws InterruptedException, IOException{
        initialize(connectString);
    }

    private void initialize(String connectString) throws InterruptedException, IOException{
        zkConnection = new ZKConnection();
        zkeeper = zkConnection.connect(connectString);
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

    public void getZNodeChildrenCallback(String path, Watcher callbackWatcher, ChildrenCallback callback) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        List<String> children;
        zkeeper.getChildren(path, callbackWatcher, callback, null);
        // return children;
    }

    public String getZNodeData(String path, boolean watch) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        byte[] b = null;
        b = zkeeper.getData(path, watch, null);
        return new String(b, "UTF-8");
    }

    public void deleteZNode(String path) throws KeeperException, InterruptedException {
        zkeeper.delete(path, -1);
    }

    public void update(String path, byte[] data) throws KeeperException, 
      InterruptedException {
        // int version = zkeeper.exists(path, true).getVersion();
        zkeeper.setData(path, data, -1);
    }
}