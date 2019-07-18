package cn.neu.hadoop.bigdata.userdefineddatatypes;

import org.apache.hadoop.io.FloatWritable;

public class DesFloatWritable extends FloatWritable {
    public DesFloatWritable() {
    }

    public DesFloatWritable(float num) {
        super(num);
    }

    @Override
    public int compareTo(FloatWritable o) {
        return -super.compareTo(o);
    }
}