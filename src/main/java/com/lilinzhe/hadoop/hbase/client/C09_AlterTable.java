package com.lilinzhe.hadoop.hbase.client;

import com.lilinzhe.hadoop.hbase.client.HbaseUtils.Utils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class C09_AlterTable {
    public static void main(String[] args) throws IOException {
        Connection conn = Utils.getConnection();
        Admin admin = Utils.getAdmin(conn);

        TableName tableName = TableName.valueOf("tb_city");
        ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes());
        cfdb.setMaxVersions(5);
        ColumnFamilyDescriptor cfd = cfdb.build();

        admin.modifyColumnFamily(tableName,cfd);

        admin.close();
        conn.close();

    }

    private static void deleteCF(Admin admin) throws IOException {
        //deleteColumnFamily()
        TableName tableName = TableName.valueOf("tb_city".getBytes());

        admin.deleteColumnFamily(tableName,"info2".getBytes());
    }

    private static void addCF(Admin admin) throws IOException {
        //addColumnFamily()
        /**
         * 参数一：TableName
         * 要删除的表
         * 参数二：ColumnFamilyDescriptor
         * 列族
         */
        TableName tableName = TableName.valueOf("tb_city");
        ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder("info2".getBytes());
        cfdb.setMaxVersions(5);
        ColumnFamilyDescriptor cfd = cfdb.build();
        admin.addColumnFamily(tableName,cfd);
    }
}