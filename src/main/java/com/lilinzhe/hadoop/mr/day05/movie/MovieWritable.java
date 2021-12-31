package com.lilinzhe.hadoop.mr.day05.movie;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 1)封装json数据
 * 2）放在了Mapper的value的位置，这个类需要序列化 使用HDP的序列化方式
 */
public class MovieWritable implements Writable {
    private String movie;
    private double rate;
    private String timeStamp;
    private String uid;

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public MovieWritable(String movie, double rate, String timeStamp, String uid) {
        this.movie = movie;
        this.rate = rate;
        this.timeStamp = timeStamp;
        this.uid = uid;
    }

    public MovieWritable() {
    }

    @Override
    public String toString() {
        return "MovieWritable{" +
                "movie='" + movie + '\'' +
                ", rate=" + rate +
                ", timeStamp='" + timeStamp + '\'' +
                ", uid='" + uid + '\'' +
                '}';
    }

    @Override
    //序列化
    //作用：进程间通信，永久存储，Hadoop节点间通信
    public void write(DataOutput dataOutput) throws IOException {
        //把每个对象序列化到输出流
        dataOutput.writeUTF(movie);
        dataOutput.writeDouble(rate);
        dataOutput.writeUTF(timeStamp);
        dataOutput.writeUTF(uid);
    }

    @Override
    //反序列化
    public void readFields(DataInput dataInput) throws IOException {
        //readFields是把输入流字节反序列化
        movie = dataInput.readUTF();
        rate = dataInput.readDouble();
        timeStamp = dataInput.readUTF();
        uid = dataInput.readUTF();
    }
}
