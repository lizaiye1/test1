package com.lilinzhe.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

//获取节点以及获取节点的值
public class Demo04 {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk = new ZooKeeper("linux01:2181", 2000, null);

        zk.setData("/a/a1" , "我是谁".getBytes(),-1);
        System.out.println("操作成功");

        zk.close();
    }
}
