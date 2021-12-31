package com.lilinzhe.hadoop.hbase.client;

import com.lilinzhe.hadoop.hbase.client.HbaseUtils.Utils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 *
 **/
public class C05_create {

    public static void main(String[] args) throws IOException {
        Connection conn = Utils.getConnection();
        Admin admin = Utils.getAdmin(conn);
        createTableMoreCF(admin);
        admin.close();
        conn.close();
    }
    /**
     *
     * @Param: [admin]
     * @Return: void
     * @Author: 李麟哲
     * @Date: 2021/12/13 16:32
     * @Description: TODO
     * 创建多个列族的表
     */
    private static void createTableMoreCF(Admin admin) throws IOException {
        //表构建器
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf("tb_02"));
        //列族构建器1
        ColumnFamilyDescriptorBuilder cfdb1 = ColumnFamilyDescriptorBuilder.newBuilder("cf1".getBytes());
        //设置存活时间
        cfdb1.setTimeToLive(60*60*4);
        //设置版本
        cfdb1.setMaxVersions(5);
        ColumnFamilyDescriptor cfd1 = cfdb1.build();

        //列族构建器2
        ColumnFamilyDescriptorBuilder cfdb2 = ColumnFamilyDescriptorBuilder.newBuilder("cf2".getBytes());
        ColumnFamilyDescriptor cfd2 = cfdb2.build();

        List<ColumnFamilyDescriptor> cfds = Arrays.asList(cfd1, cfd2);
        tdb.setColumnFamilies(cfds);

        TableDescriptor  tableDescriptor = tdb.build();
        admin.createTable(tableDescriptor);
    }

    private static void createTableOneCF(Admin admin) throws IOException {
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("tb_01"));
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("cf1".getBytes());
        ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        TableDescriptor  tableDescriptor = tableDescriptorBuilder.build();
        admin.createTable(tableDescriptor);
    }
}