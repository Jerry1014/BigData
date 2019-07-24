package cn.neu.hadoop.bigdata.userdefineddatatypes;

import org.apache.hadoop.io.IntWritable;

public class DesIntWritable extends IntWritable {
    public DesIntWritable() {
    }

    public DesIntWritable(int num) {
        super(num);
    }

    @Override
    public int compareTo(IntWritable o) {
        return -super.compareTo(o);
    }
}
