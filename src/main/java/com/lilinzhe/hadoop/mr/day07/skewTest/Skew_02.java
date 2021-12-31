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

public class Skew_02 {
    static class SkewMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        Text k = new Text();
        IntWritable v = new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");
            String[] split1 = split[0].split("-");
            String word = split1[0];
            k.set(word);
            int i = Integer.parseInt(split[1]);
            v.set(i);
            context.write(k,v);
        }
    }
    static class SkewReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        IntWritable v = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int cnt = 0;
            for (IntWritable value : values) {
                cnt += value.get();
            }
            v.set(cnt);
            context.write(key,v);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf , "skew");
        job.setMapperClass(SkewMapper.class);
        job.setReducerClass(SkewReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("e://mrdata//skew//output//skew_res1"));
        FileOutputFormat.setOutputPath(job,new Path("e://mrdata//skew//output2//skew_res2"));
        job.waitForCompletion(true);
    }
}
