package hbase.load;

/**
 * @DATE 2021/12/16 10:43
 * @VERSION 1.0
 * @Author lilinzhe
 * @Describe TODO
 */

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieBean implements Writable {
    private  String  movie ;
    private  double  rate ;
    private  String timeStamp ;
    private  String uid ;

    /**
     * 注意保留空参构造器
     * @return
     */
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

    @Override
    public String toString() {
        return "Movie{" +
                "movie='" + movie + '\'' +
                ", rate=" + rate +
                ", timeStamp='" + timeStamp + '\'' +
                ", uid='" + uid + '\'' +
                '}';
    }
    // 序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(movie);
        dataOutput.writeDouble(rate);
        dataOutput.writeUTF(timeStamp);
        dataOutput.writeUTF(uid);

    }
    // 反序列化
    public void readFields(DataInput dataInput) throws IOException {
        // 和写出的数据类型和顺序保证一致
        movie = dataInput.readUTF();
        rate =dataInput.readDouble();
        timeStamp = dataInput.readUTF();
        uid = dataInput.readUTF() ;

    }

}
