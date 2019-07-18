package cn.neu.hadoop.bigdata.LPA;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;

@Component
@Slf4j
public class LPAClassificationViewer {
    public static class LPAViewerMapper extends Mapper<Object, Text, IntWritable, Text> {
        static HashMap<String, Integer> appear_label = new HashMap<>();
        static int label_no = 0;

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] key_value = values.toString().split("\t");
            String[] name_label = key_value[0].split("#");

            if (appear_label.containsKey(name_label[1]))
                context.write(new IntWritable(appear_label.get(name_label[1])), new Text(name_label[0]));
            else {
                appear_label.put(name_label[1], label_no);
                context.write(new IntWritable(label_no), new Text(name_label[0]));
                label_no++;
            }
        }
    }

    public static class LPAViewerReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text i : values) {
                context.write(key, i);
            }
        }
    }

    public static void main(String input_path, String output_path) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(LPAClassificationViewer.class);
        job.setMapperClass(LPAViewerMapper.class);
        job.setReducerClass(LPAViewerReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));
        job.waitForCompletion(true);
    }
}