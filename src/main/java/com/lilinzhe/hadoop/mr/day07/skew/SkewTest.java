package com.lilinzhe.hadoop.mr.day07.skew;

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

/**
 * 该页代码展示发生数据倾斜时的情况
 */
public class SkewTest {
    static class SkewMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] split = s.split(" ");
            for (String s1 : split) {
                k.set(s1);
                context.write(k,v);
            }

        }
    }
    static class SkewReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(key,value);
            }
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
