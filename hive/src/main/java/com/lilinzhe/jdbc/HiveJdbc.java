package com.lilinzhe.jdbc;

import java.sql.*;

/**
 * @Date 2021/12/23
 * @Created by
 * @Description
 */
public class HiveJdbc {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws SQLException {
//        //注册驱动
//        try {
//            Class.forName(driverName);
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
        //获取连接
        Connection conn = DriverManager.getConnection("jdbc:hive2://linux01:10000/doit28","root","");
        //获取指定sql的对象
        Statement statement = conn.createStatement();
        String sql = "select * from movie";

        //执行sql
        ResultSet res = statement.executeQuery(sql);
        //处理结果
        while(res.next()){
            System.out.println(res.getString(1)+"_"+res.getString(2));
        }
        conn.close();
    }
}