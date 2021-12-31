package com.lilinzhe.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

//获取节点以及获取节点的值
public class Demo05 {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk = new ZooKeeper("linux01:2181", 2000, null);
        Stat exists = zk.exists("/a", null);
        if(exists != null){
            rmr("/a",zk);

        }
        System.out.println("操作成功");

        zk.close();
    }
    public static void rmr(String path , ZooKeeper zk) throws InterruptedException, KeeperException {
        List<String> children = zk.getChildren(path, null);
        if (children != null && children.size() >0){
            for (String child : children) {
                rmr(path + "/" + child , zk);
            }
        }
        zk.delete(path,-1);

    }
}
