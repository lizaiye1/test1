package com.lilinzhe.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class C01_insert {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux01:2181,linux02:2181,linux03:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("tb_stu"));
        Put put = new Put("rk002".getBytes());
        put.addColumn("cf".getBytes(),"name".getBytes(),"john".getBytes());
        put.addColumn("cf".getBytes(),"age".getBytes(),"33".getBytes());
        put.addColumn("cf".getBytes(),"gender".getBytes(),"female".getBytes());
        table.put(put);
        table.close();
        conn.close();


    }
}
