package com.lilinzhe.hadoop.hbase.load;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @DATE 2021/12/16 10:43
 * @VERSION 1.0
 * @Author lilinzhe
 * @Describe TODO
 */
public class LoadMovie {
    static class LoadMapper extends Mapper<LongWritable,Text,Text, MovieBean>{
        Text k = new Text();
        Gson gson = new Gson();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MovieBean>.Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                MovieBean movieBean = gson.fromJson(line, MovieBean.class);

                //获取电影id
                String mid = movieBean.getMovie();
                //定长电影id
                String newMid = StringUtils.leftPad(mid, 6, "0");
                //获取时间
                String timeStamp = movieBean.getTimeStamp();
                //定长时间
                String newTime = StringUtils.leftPad(timeStamp,10,"0");
                //生成rowkey
                String rowKey = newMid + "_" + newTime  ;
                //设置k
                k.set(rowKey);
                context.write(k,movieBean);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 将数据直接写入到Hbase表中
     *   Put 将每条数据封装在Put中
     */
    static class  LoadReducer extends TableReducer<Text,MovieBean, ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key, Iterable<MovieBean> values, Reducer<Text, MovieBean, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            //获取行键rk
            String rk = key.toString();
            //获取rk对应的moviebean
            //因为每个rk都只对应一个mb，所以不用遍历了
            MovieBean movieBean = values.iterator().next();
            //获取数据属性
            String movie = movieBean.getMovie();
            double rate = movieBean.getRate();
            String timeStamp = movieBean.getTimeStamp();
            String uid = movieBean.getUid();
            //创建put
            Put put = new Put(Bytes.toBytes(rk));
            //添加单元格
            put.addColumn("cf".getBytes(), "movie".getBytes(), Bytes.toBytes(movie));
            put.addColumn("cf".getBytes(), "rate".getBytes(), Bytes.toBytes(rate));
            put.addColumn("cf".getBytes(), "timeStamp".getBytes(), Bytes.toBytes(timeStamp));
            put.addColumn("cf".getBytes(), "uid".getBytes(), Bytes.toBytes(uid));
            //写数据
            context.write(null,put);
        }

        public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "linux01:2181,linux02:2181,linux03:218");
            Job job = Job.getInstance(conf,"load");
            job.setMapperClass(LoadMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(MovieBean.class);
            FileInputFormat.setInputPaths(job,new Path("E:\\mrdata\\movie\\input\\"));
            TableMapReduceUtil.initTableReducerJob("tb_movie" , LoadReducer.class,job);
            job.waitForCompletion(true);
        }
    }
}