package me.calvinliu.zookeeper.example;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

/**
 * ZooKeeper Client Operation
 * Created by calvinliu on 7/6/2016.
 */
public class ZooKeeperClientOperation {
    // create static instance for zookeeper class.
    private static ZooKeeper zk;

    // Method to create znode in zookeeper ensemble
    public static void create(String path, byte[] data) throws KeeperException, InterruptedException {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public static void main(String[] args) {
        // znode path
        String path = "/MyFirstZnode"; // Assign path to znode
        // data in byte array
        byte[] data = "My first zookeeper app".getBytes(); // Declare data
        try {
            ZooKeeperConnection conn = new ZooKeeperConnection();
            zk = conn.connect("localhost");
            // Create the data to the specified path
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            // Check the Existence
            Stat stat = zk.exists(path, true);

            // Get Data
            if (stat != null) {
                final CountDownLatch connectedSignal = new CountDownLatch(1);
                byte[] bytes = zk.getData(path, new Watcher() {
                    public void process(WatchedEvent we) {
                        if (we.getType() == Event.EventType.None) {
                            switch (we.getState()) {
                                case Expired:
                                    connectedSignal.countDown();
                                    break;
                            }
                        } else {
                            String path = "/MyFirstZnode";
                            try {
                                byte[] bn = zk.getData(path, false, null);
                                // List children
                                zk.getChildren(path, false).forEach(System.out::println);
                                String data = new String(bn, "UTF-8");
                                System.out.println(data);
                                connectedSignal.countDown();
                            } catch (Exception ex) {
                                System.out.println(ex.getMessage());
                            }
                        }
                    }
                }, null);
                System.out.println(new String(bytes, "UTF-8"));
                connectedSignal.await();

                // Set Data
                zk.setData(path, data, zk.exists(path, true).getVersion());

                // Delete Data
                zk.delete(path,zk.exists(path,true).getVersion());
            } else {
                System.out.println("Node does not exists");
            }
            conn.close();
        } catch (Exception e) {
            System.out.println(e.getMessage()); // Catch error message
        }
    }
}
