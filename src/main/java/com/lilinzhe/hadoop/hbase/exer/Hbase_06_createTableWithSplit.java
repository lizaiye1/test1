package com.lilinzhe.hadoop.hbase.exer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @DATE 2021/12/15 22:20
 * @VERSION 1.0
 * @Author lilinzhe
 * @Describe TODO
 * 建表 admin.createTable(TableDescriptor td , byte[][] keys)
 * 第一个参数：表描述器
 * 第二个参数：分区region的多个行键组成的字节型数组
 */
public class Hbase_06_createTableWithSplit {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "linux01:2181,linux02:2181,linux03:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf("tb_java02"));

        ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"));
        cfdb.setMaxVersions(5);
        ColumnFamilyDescriptor cfd = cfdb.build();
        tdb.setColumnFamily(cfd);
        TableDescriptor td = tdb.build();

        byte[][] splitKeys = new byte[][]{Bytes.toBytes('F'),Bytes.toBytes('V')};

        admin.createTable(td,splitKeys);
    }
}