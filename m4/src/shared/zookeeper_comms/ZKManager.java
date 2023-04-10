package shared.zookeeper_comms;

import java.io.UnsupportedEncodingException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

public interface ZKManager {
  public void create(String path, byte[] data)
      throws KeeperException, InterruptedException;

  public String getZNodeDataCallback(String path, Watcher callbackWatcher)
      throws KeeperException, InterruptedException, UnsupportedEncodingException;

  public String getZNodeData(String path, boolean watch)
      throws KeeperException, InterruptedException, UnsupportedEncodingException;

  public void update(String path, byte[] data)
      throws KeeperException, InterruptedException;
}
