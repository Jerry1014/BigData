package cn.neu.hadoop.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
         * key 为Map中的key，hadoop会把相同key的内容合并为一个list，该list就为values。
         * context为存放结果的对象
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                //累加每一个value
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }