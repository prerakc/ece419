package runnables;

import java.lang.Runnable;
import app_kvServer.KVServer;
import ecs.ECSNode;

public class ShutdownRunnable implements Runnable {

   ECSNode node;

   public ShutdownRunnable(ECSNode node) {
      // store parameter for later user
      this.node = node;
   }

   public void run() {
      KVServer.handleShutdown(this.node);
   }

}