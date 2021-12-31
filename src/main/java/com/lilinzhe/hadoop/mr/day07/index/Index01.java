package com.lilinzhe.hadoop.mr.day07.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * 需求：各个字母要统计各个文件内出现的次数
 * Index01中：
 * 先通过map将key设置为”单词-文件名的格式“
 * reduce统计出《K，V》 <单词-文件名，数量>
 */
public class Index01 {
    static class Index01Mapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        String fileName = null ;
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }


        Text k = new Text();
        IntWritable v = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split("\\s+");
            for (String word : splits) {
                String newKey = word + "-" + fileName;
                k.set(newKey);
                context.write(k,v);
            }
        }
    }
    static class Index01Reducer extends Reducer<Text,IntWritable,Text,IntWritable>{

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
        Job job = Job.getInstance(conf , "index_01");
        job.setMapperClass(Index01Mapper.class);
        job.setReducerClass(Index01Reducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job , new Path("E:\\mrdata\\index\\input"));
        FileOutputFormat.setOutputPath(job , new Path("E:\\mrdata\\index\\output_res01"));
        job.waitForCompletion(true);
    }
}
