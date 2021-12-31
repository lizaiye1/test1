package hbase.client02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;

import java.io.IOException;
import java.util.Iterator;

/**
 * @DATE 2021/12/16 14:57
 * @VERSION 1.0
 * @Author lilinzhe
 * @Describe TODO
 */
public class Hbase_02_Filter {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux01:2181,linux02:2181,linux03:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("tb_teacher"));
        Scan scan = new Scan();
        //设置过滤器,第一个参数 运算符
        ValueFilter valueFilter = new ValueFilter(CompareOperator.EQUAL, new BinaryComparator("llz".getBytes()));
        scan.setFilter(valueFilter);

        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            while(result.advance()){
                Cell cell = result.current();
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] row = CellUtil.cloneRow(cell);
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