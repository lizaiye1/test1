package com.lilinzhe.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class C02_delete {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux01:2181,linux02:2181,linux03:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("tb_stu"));
        //删除一整行
        Delete delete = new Delete("rk002".getBytes());
//        delete.addColumn("cf".getBytes(),"name".getBytes());
        delete.addFamily("cf".getBytes());

        table.delete(delete);
        table.close();
        conn.close();


    }
}
