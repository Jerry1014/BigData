package cn.neu.hadoop.bigdata.LPA;

import cn.neu.hadoop.bigdata.UserDefinedDataTypes.DesFloatWritable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;

@Component
@Slf4j
public class LPAReorganize {
    public static class LPAReorganizeMapper extends Mapper<Object, Text, DesFloatWritable, Text> {
        static HashMap<String, Integer> label_label_no = new HashMap<>();
        static int next_no = 0;

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] key_value = values.toString().split("\t");
            float PR = Float.valueOf(key_value[1].split("#")[0]);
            String[] name_label = key_value[0].split("#");

            int this_label;
            if (label_label_no.containsKey(name_label[1])) this_label = label_label_no.get(name_label[1]);
            else {
                label_label_no.put(name_label[1], next_no);
                this_label = next_no;
                next_no++;
            }
            context.write(new DesFloatWritable(PR), new Text(name_label[0] + '#' + next_no));
//            context.write(new DesFloatWritable(PR), new Text(name_label[0]));
        }
    }

    public static class LPAReorganizePartitioner extends Partitioner<DesFloatWritable, Text> {
        HashMap<String, Integer> label_label_no = new HashMap<>();
        static int next_no = 0;

        @Override
        public int getPartition(DesFloatWritable desFloatWritable, Text text, int i) {
            String[] name_label = text.toString().split("#");
//            String[] name_label = {"",text.toString()};
            if (label_label_no.containsKey(name_label[1])) {
                return label_label_no.get(name_label[1]);
            } else {
                label_label_no.put(name_label[1], next_no);
                next_no++;
                return next_no - 1;
            }
        }
    }

    public static class LPAReorganizeReduce extends Reducer<DesFloatWritable, Text, DesFloatWritable, Text> {
        public void reduce(DesFloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text i : values) {
                context.write(key, i);
            }
        }
    }

    public static void main(String input_path, String output_path) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(LPAReorganize.class);
        job.setMapperClass(LPAReorganizeMapper.class);
        job.setPartitionerClass(LPAReorganizePartitioner.class);
        job.setReducerClass(LPAReorganizeReduce.class);
        job.setOutputKeyClass(DesFloatWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));
//        job.setNumReduceTasks(500);
        job.waitForCompletion(true);
    }
}
