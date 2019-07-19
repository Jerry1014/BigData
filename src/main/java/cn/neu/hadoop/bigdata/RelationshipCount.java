package cn.neu.hadoop.bigdata;

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

@Component
@Slf4j
public class RelationshipCount {
    private static String input_path = "/test/input";
    private static String output_path = "/test/output2";

    public static class NameCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] all_name = value.toString().split(" ");
            for (int i = 0; i < all_name.length; i++) {
                String first_person = all_name[i];
                for (int j = i + 1; j < all_name.length; j++) {
                    String second_person = all_name[j];
                    if (first_person.equals(second_person)) continue;
                    if (first_person.compareTo(second_person) > 0)
                        context.write(new Text(first_person + ',' + second_person), new IntWritable(1));
                    else context.write(new Text(second_person + ',' + first_person), new IntWritable(1));
                }
            }
        }
    }

    public static class NameCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String name_node) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(RelationshipCount.class);
        job.setMapperClass(NameCountMapper.class);
        job.setCombinerClass(RelationshipCount.NameCountReducer.class);
        job.setReducerClass(RelationshipCount.NameCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(name_node + input_path));
        FileOutputFormat.setOutputPath(job, new Path(name_node + output_path));
        job.waitForCompletion(true);
    }

    public static void main(String in_path, String out_path, String name_node) throws IOException, InterruptedException, ClassNotFoundException {
        input_path = in_path;
        output_path = out_path;
        main(name_node);
    }
}
