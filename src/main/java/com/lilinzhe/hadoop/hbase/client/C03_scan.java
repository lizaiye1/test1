package com.lilinzhe.hadoop.hbase.client;

import com.lilinzhe.hadoop.hbase.client.HbaseUtils.Utils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Iterator;


public class C03_scan {
    public static void main(String[] args) throws IOException {
        Connection conn = Utils.getConnection();
        Table table = Utils.getTable(conn, "tb_stu");


        Scan scan = new Scan();

        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result result = iterator.next();
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
        table.close();
        conn.close();
    }
}
