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
import java.util.Random;

/**
 * 该类代码与SkewTest02，组成两个MR程序，来实现解决数据倾斜的情况
 *
 * 逻辑：
 * 第一个MR程序，
 * 在map阶段，先获取reduceTask的数量，以此为范围，给每个key的值后面都附上一个该区间的随机数
 *
 */
public class SkewTest01 {
    static class SkewMapper01 extends Mapper<LongWritable, Text,Text, IntWritable> {

        //获取一个reduceTask的数量

        int num = 0;
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            num = context.getNumReduceTasks();
        }
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        Random random = new Random();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] split = s.split("\\s+");
            for (String s1 : split) {
                //先获取一个随机数
                int i = random.nextInt(num);//num是repreduceTask个数
                String newKey = s1 + "-" + i;
                k.set(newKey);
                context.write(k,v);
            }
        }
    }
    static class SkewReducer01 extends Reducer<Text,IntWritable,Text,IntWritable>{
        IntWritable v = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int cnt = 0;
            for (IntWritable value : values) {
                cnt ++;
            }
            v.set(cnt);
            context.write(key,v);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Skew");
        job.setMapperClass(SkewMapper01.class);
        job.setReducerClass(SkewReducer01.class);
        job.setNumReduceTasks(2);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\mrdata\\skew\\input\\"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\mrdata\\skew\\output\\skew_res7"));
        job.waitForCompletion(true);
    }
}
