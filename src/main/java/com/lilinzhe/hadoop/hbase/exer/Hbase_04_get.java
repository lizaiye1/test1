package com.lilinzhe.hadoop.hbase.exer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @DATE 2021/12/15 21:41
 * @VERSION 1.0
 * @Author lilinzhe
 * @Describe TODO
 */
public class Hbase_04_get {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "linux01:2181,linux02:2181,linux03:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("tb_actor"));
        Get get = new Get(Bytes.toBytes("rk001"));
        get.addFamily(Bytes.toBytes("cf"));
        Result result = table.get(get);
        while (result.advance()) {
            Cell cell = result.current();
            //使用的是CellUtil的工具类
            byte[] row = CellUtil.cloneRow(cell);
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);
            System.out.println(
                    new String(row) + ":" +
                            new String(family) + ":" +
                            new String(qualifier) + ":" +
                            new String(value)
            );
        }
        table.close();
        conn.close();

    }
}