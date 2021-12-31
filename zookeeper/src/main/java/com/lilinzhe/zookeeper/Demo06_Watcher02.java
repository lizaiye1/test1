package com.lilinzhe.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * 数据监听
 */
public class Demo06_Watcher02 {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk = new ZooKeeper("linux01:2181", 2000, null);
        List<String> children = zk.getChildren("/a", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("该节点的子节点个数发生改变。。。" + watchedEvent.getType());
                try {
                    List<String> children1 = zk.getChildren("/a", this);

                    System.out.println("子节点：" + children1);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        System.out.println("子节点：" + children);

        Thread.sleep(Integer.MAX_VALUE);
//        zk.close();
    }
}
