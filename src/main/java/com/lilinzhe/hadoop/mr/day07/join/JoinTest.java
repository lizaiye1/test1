package com.lilinzhe.hadoop.mr.day07.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class JoinTest {
    static class JoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
        Map<String,String> map = new HashMap<>();
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new FileReader("user.txt"));
            String line = null;
            while((line = br.readLine()) != null){
                String uid = br.readLine().split(",")[0];
                map.put(uid,line);
            }
        }

        Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\\s+");
            String uid = split[1];
            String user = map.get(uid);
            String res = line + "," + user ;
            k.set(res);
            context.write(k,NullWritable.get());
        }




    }
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "yarn");
        Job job = Job.getInstance(conf, "join_withouReduce");
        job.setJarByClass(JoinTest.class);
        job.addCacheFile(new URI("hdfs://linux01:8020/data/user.txt"));
        job.setMapperClass(JoinMapper.class);
//            job.setReducerClass(JoinReducer.class);
//            job.setNumReduceTasks(2);
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("/data/join/"));
        FileOutputFormat.setOutputPath(job, new Path("/data/join_res/"));
        job.waitForCompletion(true);
    }
}
