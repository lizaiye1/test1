package com.lilinzhe.hadoop.hbase.client;

import com.lilinzhe.hadoop.hbase.client.HbaseUtils.Utils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class C08_DropTable {
    public static void main(String[] args) throws IOException {
        Connection conn = Utils.getConnection();
        Admin admin = Utils.getAdmin(conn);
        TableName tb_city = TableName.valueOf("tb_city02".getBytes());
        //判断表是否存在
        boolean exists = admin.tableExists(tb_city);
        if (exists){
            //禁用
            boolean isEnabled = admin.isTableEnabled(tb_city);
            if (isEnabled){
                admin.disableTable(tb_city);
            }
            //删
            admin.deleteTable(tb_city);
            System.out.println("删除成功");
        }else{
            System.out.println("没有该表。。。。");
        }



        admin.close();
        conn.close();

    }
}