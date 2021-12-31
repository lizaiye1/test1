package com.lilinzhe.hadoop.hbase.exer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @DATE 2021/12/15 21:56
 * @VERSION 1.0
 * @Author lilinzhe
 * @Describe TODO
 * 建表   admin.createTable(TableDescriptor td);
 * 第一个参数：表描述器
 */
public class Hbase_05_createTable {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "linux01:2181,linux02:2181,linux03:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        //建立表描述器构建器
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf("tb_java"));

        //设置列族
        ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"));
        cfdb.setMaxVersions(3);
        ColumnFamilyDescriptor cfd = cfdb.build();
        //把列族表述器添加进 ------> 表描述器构建器
        tdb.setColumnFamily(cfd);
        //通过表表述器构建器，build出表描述器
        TableDescriptor td = tdb.build();
        //admin建表
        admin.createTable(td);

        admin.close();
        conn.close();


    }
}