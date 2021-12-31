package com.lilinzhe.hadoop.hbase.client;


import com.lilinzhe.hadoop.hbase.client.HbaseUtils.Utils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class C04_get {
    public static void main(String[] args) throws Exception {
        Connection conn = Utils.getConnection();
        Table table = Utils.getTable(conn, "tb_stu");
        getMoreRow(table);
        table.close();
        conn.close();
    }
    private static void getMoreRow(Table table) throws IOException {
        Get get1 = new Get("rk003".getBytes());
        get1.addColumn("cf".getBytes(),"age".getBytes());
        Get get2 = new Get("002".getBytes());
        List<Get> gets = Arrays.asList(get1, get2);
        Result[] results = table.get(gets);
        for (Result result : results) {

            Utils.getRow(result);
        }
    }

    private static void getOneRow(Table table) throws IOException {
        Get get = new Get("rk003".getBytes());
        get.addColumn("cf".getBytes(),"age".getBytes());
        Result result = table.get(get);
        Utils.getRow(result);
    }
}
