package com.lilinzhe.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * 删除一个znode,节点
 */
public class Demo01 {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk = new ZooKeeper("linux01:2181,linux02:2181,linux03:2181",2000,null);
        zk.delete("/servers/server",-1);
        zk.close();

    }
}
