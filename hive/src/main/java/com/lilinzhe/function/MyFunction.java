package com.lilinzhe.function;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Date 2021/12/22
 * @Created by
 * @Description
 */
public class MyFunction extends UDF {
    public String evaluate(int  i , String...str){
        StringBuffer sb = new StringBuffer();
        for (String s : str) {
            sb.append(s.toUpperCase()) ;
        }
        return   sb.toString()+i ;

    }
}