package com.lilinzhe.hadoop.hbase.client;

import com.lilinzhe.hadoop.hbase.client.HbaseUtils.Utils;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class C07_CreateNS {
    public static void main(String[] args) throws IOException {
        Connection conn = Utils.getConnection();
        Admin admin = Utils.getAdmin(conn);

        //nsd
        NamespaceDescriptor.Builder doit128 = NamespaceDescriptor.create("doit128");
        NamespaceDescriptor namespaceDescriptor = doit128.build();

        //创建namespace
        admin.createNamespace(namespaceDescriptor);

        admin.close();
        conn.close();
    }
}