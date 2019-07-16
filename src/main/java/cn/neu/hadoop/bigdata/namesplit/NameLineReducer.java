package cn.neu.hadoop.bigdata.namesplit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class NameLineReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder name_list = new StringBuilder();
        for (Text i : values) {
            name_list.append(i.toString());
            name_list.append(' ');
        }
        context.write(NullWritable.get(), new Text(name_list.toString()));
    }
}
