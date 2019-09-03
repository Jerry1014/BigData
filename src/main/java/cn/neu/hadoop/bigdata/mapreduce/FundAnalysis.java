package cn.neu.hadoop.bigdata.mapreduce;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class FundAnalysis {
    public static class AllIncomeMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (((LongWritable) key).get() != 0) {
                String[] income_str = value.toString().split(",");
                for (int i = 2; i < 7; i++) {
                    String income = income_str[i];
                    if (income.length() < 3) return;
                    context.write(new IntWritable(i - 2), new Text(income.substring(0, income.length() - 1)));
                }
            }
        }
    }

    public static class AllIncomePartitioner extends Partitioner<IntWritable, Text> {
        @Override
        public int getPartition(IntWritable intWritable, Text text, int i) {
            return intWritable.get();
        }
    }

    public static class AllIncomeCombiner extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text i : values) {
                context.write(new IntWritable(Integer.parseInt(i.toString())), new IntWritable(1));
            }
        }
    }

    public static class IncomeClassificationReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String in_path, String out_path, String name_node) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(FundAnalysis.class);
        job.setMapperClass(AllIncomeMapper.class);
        job.setCombinerClass(AllIncomeCombiner.class);
        job.setReducerClass(IncomeClassificationReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);//设置reduce的个数
        FileInputFormat.addInputPath(job, new Path(name_node + in_path));
        FileOutputFormat.setOutputPath(job, new Path(name_node + out_path));
        job.waitForCompletion(true);
    }
}
