package com.lilinzhe.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * 数据监听
 */
public class Demo06_Watcher {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk = new ZooKeeper("linux01:2181", 2000, null);
        byte[] data1 = zk.getData("/a", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("数据发生了改变");
                try {
                    byte[] data2 = zk.getData("/a", this, null);
                    System.out.println("更改后的值为： " + new String(data2));
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }, null);

        Thread.sleep(Integer.MAX_VALUE);
//        zk.close();
    }
}
