package shared.zookeeper_comms;

import java.util.concurrent.CountDownLatch;
import java.io.IOException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ZKConnection {
    private ZooKeeper zk;
    CountDownLatch connectionLatch = new CountDownLatch(1);
    
    public ZooKeeper connect(String host) throws IOException, InterruptedException {
        zk = new ZooKeeper(host, 2000, new Watcher() {
        public void process(WatchedEvent we) {
            if (we.getState() == KeeperState.SyncConnected) {
                connectionLatch.countDown();
            }
          }
      });

      connectionLatch.await();
      return zk;
  }

  public void close() throws InterruptedException {
      zk.close();
  }
}

