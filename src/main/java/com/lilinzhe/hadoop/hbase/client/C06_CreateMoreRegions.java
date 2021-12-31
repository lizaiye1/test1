package com.lilinzhe.hadoop.hbase.client;

import com.lilinzhe.hadoop.hbase.client.HbaseUtils.Utils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;


public class C06_CreateMoreRegions {
    public static void main(String[] args) throws IOException {
        Connection conn = Utils.getConnection();
        Admin admin = Utils.getAdmin(conn);
        //获取tableDescriptorBuild
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf("tb_city02"));
        //获取列族一构建器
        ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes());
        //设置并获取cfd
        cfdb.setMaxVersions(3);
        ColumnFamilyDescriptor cfd = cfdb.build();
        //设置tdb
        tdb.setColumnFamily(cfd);

        //创建td
        TableDescriptor td = tdb.build();//多态 父类或者接口的引用指向子类或者实现类的对象
        //设置keys
        byte[][] keys = new byte[][]{"h".getBytes(),"v".getBytes()};
        //创建表
        admin.createTable(td,keys);
        //关闭资源
        admin.close();
        conn.close();
    }
}