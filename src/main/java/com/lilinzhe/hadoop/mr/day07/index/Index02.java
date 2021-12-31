package com.lilinzhe.hadoop.mr.day07.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 使用Index01的输出<K,V>
 * 在Index02中：
 * 先通过map将<单词-文件名，数量>
 * 变成<单词，文件名：数量>的格式
 * 在通过reduce统计上面的<K,V>
 */
public class Index02 {
    static class Index02Mapper extends Mapper<Text , IntWritable, Text, Text>{
        Text k = new Text();
        Text v = new Text();
        @Override
        protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
            String line = key.toString();
            String[] split = line.split("-");
            String word = split[0];
            String fileName = split[1];
            int cnt = value.get();
            k.set(word);
            v.set(fileName + "-" + cnt);
            context.write(k,v);

        }

    }
    static class Index02Reducer extends Reducer<Text , Text , Text , Text>{

        Text v = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(" " +value.toString());
                v.set(sb.toString());
            }
            context.write(key,v);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf , "index_01");
        job.setMapperClass(Index02Mapper.class);
        job.setReducerClass(Index02Reducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.setInputPaths(job , new Path("E:\\mrdata\\index\\output_res01"));
        FileOutputFormat.setOutputPath(job , new Path("E:\\mrdata\\index\\output_res02"));
        job.waitForCompletion(true);
    }
}
