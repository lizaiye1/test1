package com.lilinzhe.hadoop.hbase.client02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 插入hbase数据方法之二：
 * 使用BufferedMutator来缓存要put的数据，缓解put请求次数过多时，服务器的压力
 */
public class C02_Mutation {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux01:2181,linux02:2181,linux03:2181");
        Connection conn = ConnectionFactory.createConnection(conf);

        BufferedMutator bufferedMutator = conn.getBufferedMutator(TableName.valueOf("tb_01"));
        Put put = new Put(Bytes.toBytes("rk001"));
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes("tom"));
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("age"),Bytes.toBytes("42"));
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("job"),Bytes.toBytes("teacher"));

        bufferedMutator.mutate(put);
        bufferedMutator.setWriteBufferPeriodicFlush(1000*60);
        bufferedMutator.flush();

        conn.close();
    }
}