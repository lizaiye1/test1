package com.lilinzhe.hadoop.mr.day06.yarn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Date 2021/12/1
 * @Created by HANGGE
 * @Description
 * KIN    单词
 * VIN    1
 * KOUT   单词
 * VOUT  总次数
 */
public class WordCountReducer  extends Reducer<Text, IntWritable , Text ,IntWritable> {
    /**
     * 必须重写reduce方法做聚合结果逻辑
     * @param key  a   c   e  g
     * @param values  一组单词  出现的1   <1,1,1,1,1,1,1,1>
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int cnt = 0 ;
        for (IntWritable value : values) {
            // 统计次数
            cnt ++ ;
        }
        // 输出结果数据
        context.write(key , new IntWritable(cnt));
    }
}