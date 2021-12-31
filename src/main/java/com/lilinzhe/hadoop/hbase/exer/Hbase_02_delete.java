package com.lilinzhe.hadoop.hbase.exer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 删除hbase表中的数据 DML语言----delete的应用
 * 其中delete对象可以调用，
 * addFamily() 删除了某一行的整个列族\
 * addColumn() 删除
 */
public class Hbase_02_delete {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux01:2181,linux02:2181,linux03:2181");
        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("tb_01"));
        Delete delete = new Delete(Bytes.toBytes("002"));
//      delete.addFamily(Bytes.toBytes("cf1"));
        delete.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"));
        delete.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("age"));
        table.delete(delete);

        table.close();
        conn.close();
    }
}