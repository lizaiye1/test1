package com.lilinzhe.hadoop.hbase.exer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * @DATE 2021/12/15 20:54
 * @VERSION 1.0
 * @Author lilinzhe
 * @Describe TODO
 * 此API是为了使用hbase的scan功能
 * 查看某表的所有数据
 */
public class Hbase_03_scan {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux01:2181,linux02:2181,linux03:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("tb_actor"));
        /**
         * 这里要注意使用的顺序
         *
         * 先新建一个Scan对象,
         * 在通过table.getScanner创建一个Scanner对象，这是一个集合，包含了表内所有的元素
         *
         */
        Scan scan = new Scan();
        //scanner包含了表内的所有元素
        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result result = iterator.next();
            //result包含了同一个行键内所有的元素
            while (result.advance()){
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
        }
        table.close();
        conn.close();

    }
}