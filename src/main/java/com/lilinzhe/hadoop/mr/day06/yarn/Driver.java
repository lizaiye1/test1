package com.lilinzhe.hadoop.mr.day06.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    /**
     * 处理hdfs上的海量数据
     * @param args
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //切换用户为root用户，如果没有就可能会报错，当前用户权限不够
        System.setProperty("HADOOP_USER_NAME","root");
        Configuration conf = new Configuration();

        //1.设置提交的模式，默认是本地
        conf.set("mapreduce.framework.name","yarn");
        //2. yarn主节点的地址
        conf.set("yarn.resourcemanager.hostname","linux01");
        //3.处理hdfs的数据

        //4.跨平台参数
        conf.set("mapreduce.app-submission.cross-platform","true");

        Job job = Job.getInstance(conf,"WordCount");
        //5.打成jar包
        job.setJar("e://wc.jar");

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job,new Path("/data/wc/"));
        FileOutputFormat.setOutputPath(job,new Path("/data/wc_res/"));
        job.waitForCompletion(true);
    }
}
