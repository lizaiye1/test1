package com.lilinzhe.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

//获取节点以及获取节点的值
public class Demo02 {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk = new ZooKeeper("linux01:2181", 2000, null);
        List<String> children = zk.getChildren("/", null);
        for (String child : children) {
            byte[] data = zk.getData("/" + child, null, null);
            System.out.println("/" + child + "   的值：   " + new String(data));

        }

        zk.close();
    }
}
