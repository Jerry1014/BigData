package cn.neu.hadoop.bigdata;

import cn.neu.hadoop.bigdata.userdefineddatatypes.DesIntWritable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class WordTop {
    public static class WordCountTopMapper extends Mapper<Object, Text, DesIntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] name_value = value.toString().split("\t");
            context.write(new DesIntWritable(Integer.valueOf(name_value[1])), new Text(name_value[0]));
        }
    }

    public static class WordCountTopReducer extends Reducer<DesIntWritable, Text, DesIntWritable, Text> {
        public void reduce(DesIntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text i : values) {
                context.write(key, i);
            }
        }
    }

    public static void main(String in_path, String out_path, String name_node) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountTopMapper.class);
        job.setReducerClass(WordCountTopReducer.class);
        job.setOutputKeyClass(DesIntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);//设置reduce的个数
        FileInputFormat.addInputPath(job, new Path(name_node + in_path));
        FileOutputFormat.setOutputPath(job, new Path(name_node + out_path));
        job.waitForCompletion(true);
    }
}

