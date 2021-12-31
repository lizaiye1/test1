package com.lilinzhe.hadoop.hbase.client02Exer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @DATE 2021/12/15 22:36
 * @VERSION 1.0
 * @Author lilinzhe
 * @Describe TODO
 * 批量插入数据:  mutation
 * 使用BufferedMutator来缓存要put的数据，缓解put请求次数过多时，服务器的压力
 *
 */
public class Hbase_01_mutation {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux01:2181,linux02:2181,linux03:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        BufferedMutator bm = conn.getBufferedMutator(TableName.valueOf("tb_java02"));
//        Table table = conn.getTable(TableName.valueOf("tb_java01"));
        Put put = new Put(Bytes.toBytes("rk001"));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("zookeeper"));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("work"),Bytes.toBytes("watch"));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("leavel"),Bytes.toBytes("easy"));

        bm.mutate(put);
        bm.setWriteBufferPeriodicFlush(1000*60);

        bm.close();
        conn.close();
    }
}