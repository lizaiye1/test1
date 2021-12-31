package com.lilinzhe.hadoop.hbase.exer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Hbase_01_put {
    public static void main(String[] args) throws IOException {
        /**
         *
         * @Param: [args]
         * @Return: void
         * @Author: 李麟哲
         * @Date: 2021/12/14 23:28
         * @Description: TODO
         * 给hbase文件表格已有列族的单元格，添加数据
         */
        //先获取配置对象
        Configuration conf = HBaseConfiguration.create();
        //设置配置链接zookeeper
        conf.set("hbase.zookeeper.quorum","linux01:2181,linux02:2181,linux03:2181");
        //获取链接
        Connection conn = ConnectionFactory.createConnection(conf);
        //创建admin对象
        Table table = conn.getTable(TableName.valueOf("tb_01".getBytes()));

        /**
         * 新建一个put对象，形参是行键
         */

        Put put1 = new Put("001".getBytes());
        /**
         * put调用addColumn方法
         * 三个参数：列族，属性名，值
         */
        put1.addColumn("cf1".getBytes(),"name".getBytes(),"marry".getBytes());
        put1.addColumn("cf1".getBytes(),"age".getBytes(),"19".getBytes());
        put1.addColumn("cf1".getBytes(),"gender".getBytes(),"female".getBytes());
        Put put2 = new Put("002".getBytes());
        /**
         * 注意这里的列族必须是已经表里已经有了的，没有的列族会报错无法添加
         */
        put2.addColumn("cf1".getBytes(),"name".getBytes(),"tom".getBytes());
        put2.addColumn("cf1".getBytes(),"age".getBytes(),"45".getBytes());
        put2.addColumn("cf1".getBytes(),"job".getBytes(),"actor".getBytes());

        /**
         * put创建好了之后，如果有很多行，可以合并成一个集合，调用table.put(放put对象)
         */
        List<Put> puts = Arrays.asList(put1, put2);
        table.put(puts);
        table.close();
        conn.close();

    }
}