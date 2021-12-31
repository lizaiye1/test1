package com.lilinzhe.hadoop.mr.day06.combiner;

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

public class CombinerTest {
    static class CombinerMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] words = s.split(" ");
            for (String word : words) {
                k.set(word);
                context.write(k,v);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        System.setProperty("HADOOP_USER_NAME","root") ;
        System.setProperty("user.name","root") ;

        //job 统计文件中出现单词的次数
        Configuration conf = new Configuration();
        //创建 job
        Job job = Job.getInstance(conf,"testcombiner");
        //Mapper类
        job.setMapperClass(CombinerMapper.class);
        //设置mapper输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置预聚合的类
        job.setCombinerClass(MyCombiner.class);

        //设置输入路径
        FileInputFormat.setInputPaths(job,new Path("E://input//word.txt"));
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("E://combiner/"));
        //提交工作
        //等待执行完成，打印执行过程
        job.waitForCompletion(true);
    }
}
