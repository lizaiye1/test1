package com.lilinzhe.hadoop.mr.day05.json;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinTest02 {
    static class JoinMapper extends Mapper<LongWritable, Text, Text , Text>{
        String fileName = null;

        /**
         * 这是获取文件名的步骤，要记住其中的方法getInpupSplit
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        /**
         * 根据文件名来判断，分开生成各自的K，V
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        Text k = new Text();
        Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String uid = null ;

            if(fileName.startsWith("user")){
                String[] split = line.split(",");
                uid = split[0];
            }else if(fileName.startsWith("order")){
                String[] split = line.split(" ");
                uid = split[1];
            }
            k.set(uid);
            v.set(line);
            context.write(k,v);
        }
    }
    static class JoinReducer extends Reducer<Text,Text,Text, NullWritable>{
        Text k = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            String user = null;
            List<String> orders = new ArrayList<>();
            for (Text value : values) {
                String content = value.toString();
                if(content.contains(",")){
                    user = content;
                }else{
                    orders.add(content);
                }
            }

            for (String order : orders) {
                String join = order.split(" ")[0] + "\t" + user;
                k.set(join);
                context.write(k,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception{
        // Job  统计文件中单词出现的次数
        Configuration conf = new Configuration();
        // 1  创建 JOB
        Job job = Job.getInstance(conf, "join");
        // 1) Mapper 类
        job.setMapperClass(JoinMapper.class);
        // 2) Reducer类
        job.setReducerClass(JoinReducer.class);
        // 3)  设置map输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 4)  设置reduce输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 5) 设置输入的数据路径
        FileInputFormat.setInputPaths(job, new Path("E:\\mrdata\\join\\input"));
        // 6) 设置输出的结果保存路径
        FileOutputFormat.setOutputPath(job, new Path("E:\\mrdata\\join\\res01"));
        // 2 提交工作
        // 等待执行完成 , 打印执行过
        job.waitForCompletion(true);
    }
}
