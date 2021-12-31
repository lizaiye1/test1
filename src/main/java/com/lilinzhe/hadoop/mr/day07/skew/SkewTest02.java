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
 * 该类代码与SkewTest01，组成两个MR程序，来实现解决数据倾斜的情况
 */
public class SkewTest02 {
    static class SkewMapper02 extends Mapper<LongWritable, Text,Text, IntWritable> {
        Text k = new Text();
        IntWritable v = new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] split = line.split("\t");

            String[] split1 = split[0].split("-");
            k.set(split1[0]);

            String str = split[1];
            int num = Integer.parseInt(str);
            v.set(num);

            context.write(k,v);
        }
    }
    static class SkewReducer02 extends Reducer<Text,IntWritable,Text,IntWritable> {
        IntWritable v = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            for (IntWritable value : values) {
                int i = value.get();
                cnt += i;
            }
            v.set(cnt);
            context.write(key,v);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Skew");
        job.setMapperClass(SkewMapper02.class);
        job.setReducerClass(SkewReducer02.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\mrdata\\skew\\output\\skew_res7"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\mrdata\\skew\\output\\skew_res9"));
        job.waitForCompletion(true);
    }
}
