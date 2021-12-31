package hbase.load;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @Date 2021/12/16
 * @Created by HANGGE
 * @Description
 * 1 读取数据
 * 2 设计rowkey   mid_time
 *     1) 定长   01228     00128   11976
 *
 */
public class LoadMovie {

    static class LoadMapper extends Mapper<LongWritable, Text ,Text , MovieBean> {
        Text k = new  Text() ;
        Gson gs = new Gson() ;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                // 解析json
                MovieBean mb = gs.fromJson(line, MovieBean.class);
                // 获取电影id
                String movie = mb.getMovie();
                // 定长电影id
                String newMid = StringUtils.leftPad(movie, 6, '0');
                // 时间
                String timeStamp = mb.getTimeStamp();
                String newTime = StringUtils.leftPad(timeStamp, 10, "0");
                String  rowkey = newMid+"_"+newTime;
                k.set(rowkey);
                // 输出数据    rk
                context.write(k , mb);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * 将数据直接写入到Hbase表中
     *   Put 将每条数据封装在Put中
     */
    static class LoadReducer extends TableReducer<Text , MovieBean , ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<MovieBean> values, Context context) throws IOException, InterruptedException {
            // 行键
            String rk = key.toString();
            // 获取rk对应的电影Bean数据
            MovieBean mb = values.iterator().next();
            // 获取数据属性
            String movie = mb.getMovie();
            double rate = mb.getRate();
            String timeStamp = mb.getTimeStamp();
            String uid = mb.getUid();
            // 创建put
            Put put = new Put(Bytes.toBytes(rk));
            // 添加单元格
            put.addColumn("cf".getBytes(), "movie".getBytes(), Bytes.toBytes(movie));
            put.addColumn("cf".getBytes(), "rate".getBytes(), Bytes.toBytes(rate));
            put.addColumn("cf".getBytes(), "timeStamp".getBytes(), Bytes.toBytes(timeStamp));
            put.addColumn("cf".getBytes(), "uid".getBytes(), Bytes.toBytes(uid));
            // 写出去
            context.write(null, put);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "linux01:2181,linux02:2181,linux03:218");
        Job job = Job.getInstance(conf);
        job.setMapperClass(LoadMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\mrdata\\movie\\input"));
        TableMapReduceUtil.initTableReducerJob("tb_movie" , LoadReducer.class,job);
        job.waitForCompletion(true);
    }
}
