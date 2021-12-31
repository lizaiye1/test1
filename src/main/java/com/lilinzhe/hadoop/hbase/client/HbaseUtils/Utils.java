package com.lilinzhe.hadoop.hbase.client.HbaseUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;


public class Utils {
    public static Connection getConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux01:2181,linux02:2181,linux03:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        return conn;
    }
    public static Table getTable(Connection conn, String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        return table;

    }

    /**
     *
     * @Param: [result]
     * @Return: void
     * @Author: 李麟哲
     * @Date: 2021/12/13 16:26
     * @Description: TODO
     */
    public static void getRow(Result result){
        while(result.advance()){
            Cell cell = result.current();
            byte[] row = CellUtil.cloneRow(cell);
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);
            System.out.println(new String(row)+":"+
                    new String(family)+":"+
                    new String(qualifier)+":"+
                    new String(value)
            );

        }
    }
    public static Admin getAdmin(Connection conn) throws IOException {
        return conn.getAdmin();

    }
}
