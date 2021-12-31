package com.lilinzhe.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class Server {
    //获取链接
    static private ZooKeeper zk = null;
    public static void  getConnection() throws IOException {
        zk = new ZooKeeper("linux01:2181,linux02:2181,linux03:2181", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("发现了新的服务器");
            }
        });
    }

    public static void register(String path,String hostname) throws InterruptedException, KeeperException {
        int i = path.lastIndexOf("/");
        String substring = null ;
        if ( i != 0){
            substring = path.substring(0, i);
        }else{
            substring = "/";
        }
        Stat exists = zk.exists(substring, null);
        if (exists == null){
            register(substring,hostname);
        }
        String s = zk.create(path,hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        if (s != null) {
            if(substring != "/"){

                System.out.println("你的宝宝上线了..");
            }
        }
    }
    public static void service() throws InterruptedException {
        System.out.println("working.....");
        Thread.sleep(Integer.MAX_VALUE);
    }

    public static void main(String[] args) {
        try {
            getConnection();
            register("/servers/server7","李麟哲7");
            service();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
