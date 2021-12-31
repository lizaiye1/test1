package com.lilinzhe.hadoop.mr.day05.json;

import com.alibaba.fastjson.JSON;
import com.lilinzhe.hadoop.mr.day05.movie.MovieWritable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


//BufferedReader br = new BufferedReader(new FileReader("E:\\mrdata\\json\\input\\rating.json"));
//        List<MovieWritable> mws = new ArrayList<>();
//        String line = null;
//        MovieWritable mw = new MovieWritable();
//        while((line = br.readLine())!=null){
//
//        mw = JSON.parseObject(line,MovieWritable.class);
//        mws.add(mw);
//        }
//        System.out.println(mws.size());
/**
 * 练习：把rating文件中的json数据格式转换成对象，并将这些java对象添加进一个集合中
 */
public class JsonDemo {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("E:\\mrdata\\json\\input\\rating.json"));
        String line = null;
        List<MovieWritable> mws = new ArrayList<>();
        MovieWritable mw = null;
        while((line = br.readLine()) != null){
            mw = JSON.parseObject(line , MovieWritable.class);
            System.out.println(mw);
            mws.add(mw);
        }
        System.out.println(mws.size());
    }
}
