package com.doitedu.mr.day06;

import com.lilinzhe.hadoop.mr.day06.combiner.MyCombiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Date 2021/12/7
 * @Created by HANGGE
 * @Description TODO
 */
public class Test {
    static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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
                context.write(kout, vout);//---> 缓存  最终输出
            }
        }
    }

    public static void main(String[] args) throws  Exception{
        // Job  统计文件中单词出现的次数
        Configuration conf = new Configuration();
        // 1  创建 JOB
        Job job = Job.getInstance(conf, "testcombiner");
        // 1) Mapper 类
        job.setMapperClass(WordCountMapper.class);

        // 4)  设置reduce输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置预聚合的类
        job.setCombinerClass(MyCombiner.class);
        // 5) 设置输入的数据路径
        FileInputFormat.setInputPaths(job , new Path("E://word.txt"));
        // 6) 设置输出的结果保存路径
        FileOutputFormat.setOutputPath(job, new Path("e://combiner_yes/"));
        // 2 提交工作
        // 等待执行完成 , 打印执行过
        job.waitForCompletion(true) ;


    }
}
