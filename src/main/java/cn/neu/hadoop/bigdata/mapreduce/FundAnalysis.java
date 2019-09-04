package cn.neu.hadoop.bigdata.mapreduce;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
public class FundAnalysis {
    public static class MonthlyIncomeClassificationMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (((LongWritable) key).get() != 0) {
                String income_str = value.toString().split(",")[2];
                if (income_str.length() < 3) return;
                int income = (int) Math.floor(Double.parseDouble(income_str.substring(0, income_str.length() - 1)));
                context.write(new IntWritable(income), new IntWritable(1));
            }
        }
    }

    public static class MonthlyIncomeClassificationReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
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
        job.setMapperClass(MonthlyIncomeClassificationMapper.class);
        job.setCombinerClass(MonthlyIncomeClassificationReducer.class);
        job.setReducerClass(MonthlyIncomeClassificationReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);//设置reduce的个数
        FileInputFormat.addInputPath(job, new Path(name_node + in_path));
        FileOutputFormat.setOutputPath(job, new Path(name_node + out_path));
        job.waitForCompletion(true);
    }
}
