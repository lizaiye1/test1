package com.lilinzhe.hadoop.mr.day06.yarn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 处理数据  行内容
        String line = value.toString();
        String[] words = line.split("\\s+");
        //将单词和1输出
        for (String word : words) {
            //输出结果数据
            Text kout = new Text(word);
            IntWritable vout = new IntWritable(1);
            context.write(kout , vout);
        }
    }
}
