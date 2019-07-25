package cn.neu.hadoop.bigdata.mapreduce.LPA;

import cn.neu.hadoop.bigdata.mapreduce.mydatatypes.DesFloatWritable;
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
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] label_and_name_pr_relationship_list = values.toString().split("\t");
            String[] name_and_pr_relation_list = label_and_name_pr_relationship_list[1].split("#");
            float PR = Float.valueOf(name_and_pr_relation_list[1]);

            context.write(new DesFloatWritable(PR), new Text(label_and_name_pr_relationship_list[0] + '#' + name_and_pr_relation_list[0]));
        }
    }

    public static class LPAReorganizePartitioner extends Partitioner<DesFloatWritable, Text> {
        HashMap<String, Integer> label_label_no = new HashMap<>();
        int next_no = 0;

        @Override
        public int getPartition(DesFloatWritable desFloatWritable, Text text, int i) {
            String[] label_name = text.toString().split("#");
            if (label_label_no.containsKey(label_name[0])) {
                return label_label_no.get(label_name[0]);
            } else {
                label_label_no.put(label_name[0], next_no);
                next_no++;
                return next_no - 1;
            }
        }
    }

    public static class LPAReorganizeReduce extends Reducer<DesFloatWritable, Text, DesFloatWritable, Text> {
        public void reduce(DesFloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text i : values) {
                String[] label_name = i.toString().split("#");
                context.write(key, new Text(label_name[1]));
            }
        }
    }

    public static void main(String input_path, String output_path, int num_cluster) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(LPAReorganize.class);
        job.setMapperClass(LPAReorganizeMapper.class);
        job.setPartitionerClass(LPAReorganizePartitioner.class);
        job.setReducerClass(LPAReorganizeReduce.class);
        job.setOutputKeyClass(DesFloatWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));
        job.setNumReduceTasks(num_cluster);
        job.waitForCompletion(true);
    }
}
