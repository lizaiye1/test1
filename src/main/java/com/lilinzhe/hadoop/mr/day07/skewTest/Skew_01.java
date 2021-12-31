package com.lilinzhe.hadoop.mr.day07.skewTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

public class Skew_01 {
    static class SkewMapper extends Mapper<LongWritable,Text,Text, IntWritable>{
        int rtNum = 0;
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            rtNum = context.getNumReduceTasks();//2
        }
        Random random = new Random();
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String newKey  = null;
            String s = value.toString();
            String[] split = s.split("\\s+");
            for (String word : split) {

                newKey = word + "-" + random.nextInt(rtNum) ;
                k.set(newKey);
                context.write(k,v);
                //<a-1 , 1 > <a-0 , 1> <a-2 , 1> <b-1 , 1> ....
                //需要注意此处的k的赋值，以及context的写出都要放在for循环内
            }
        }
    }
    static class SkewReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        IntWritable v = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int cnt =0;
            for (IntWritable value : values) {
                cnt ++;

            }
            v.set(cnt);
            context.write(key,v);
            //注意这里赋值和context的输出都不能放在for循环里


        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Skew");
        job.setMapperClass(SkewMapper.class);
        job.setReducerClass(SkewReducer.class);
        job.setNumReduceTasks(2);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\mrdata\\skew\\input\\"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\mrdata\\skew\\output\\skew_res1"));
        job.waitForCompletion(true);
    }
}
