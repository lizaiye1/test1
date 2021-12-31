package com.lilinzhe.hadoop.mr.day06.yarn;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 这里的driver类可以处理hdfs上的文件
 */
public class DriverTest {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME","root");
        //第一步，创建一个configuration对象
        //Configuration的作用之一：读取默认文件和读取site级别的文件：例如hdfs-site.xml yarn-site.xml core-site.xml
        //例如在core-site.xml文件中就可以获取到fs.defaultFS的配置信息
        //作用之二:设置一些参数
        Configuration conf = new Configuration();
        //第二步，设置配置参数
        //一、配置的是yarn
        conf.set("mapreduce.frame.work","yarn");
        //二、设置yarn主节点的位置（主机名）
        conf.set("yarn.resourcemanager.hostname","linux01");
        //三、跨平台参数
        //也可以在mapred-site.xml中将mapreduce.app-submission.cross-platform属性设置成true。
        conf.set("mapreduce.app-submission.cross-platform","true");

        //第三步，根据这些配置生成一个job
        Job job = Job.getInstance(conf,"wordcount");

        //第四步，设置程序运行的jar包路径
        job.setJar("e://wc1.jar");

        //第五步：设置相应的Mapper类，Reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //第六步：设置相应的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //第七步：设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("hdfs://linux01://data//wc//"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://linux01://data//wc_res03//"));
        //提交job
        job.waitForCompletion(true);
    }
}
