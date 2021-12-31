package com.lilinzhe.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Client {
    private static ZooKeeper zk = null;

    public static void getConnection() throws IOException {
        zk = new ZooKeeper("linux01:2181,linux02:2181,linux03:2181", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("客户端上线。。。");
            }
        });
    }
    public static void getServers() throws InterruptedException, KeeperException {

        List<String> lists = zk.getChildren("/servers", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                System.out.println("服务器新增用户。。。" + watchedEvent.getType());
                try {
                    List<String> children = zk.getChildren("/servers", this);
                    List<String> hostnames = new ArrayList<>();
                    for (String child : children) {
                        byte[] data = zk.getData("/servers/" + child, null, null);
                        String hostname = "/servers/" + child +  " : " +new String(data);
                        hostnames.add(hostname);
                    }
                    System.out.println("以注册的服务器有：" + hostnames);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        System.out.println("服务器有：" + lists);
    }
    public static void service() throws InterruptedException {
        System.out.println("Cliet开始工作。。");
        Thread.sleep(Integer.MAX_VALUE);
    }

    public static void main(String[] args)  {
        try {
            getConnection();
            getServers();
            service();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

}
