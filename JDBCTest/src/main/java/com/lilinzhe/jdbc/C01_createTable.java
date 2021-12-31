package com.lilinzhe.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Date 2021/12/23
 * @Created by
 * @Description
 */
public class C01_createTable {
    public static void main(String[] args) {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb_01", "root", "root");
            statement = conn.createStatement();
            statement.executeUpdate("create table my_student (id int primary key auto_increment,name varchar(20) )");
            System.out.println("创建表成功");
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(statement != null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}