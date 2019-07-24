package cn.neu.hadoop.bigdata;

import cn.neu.hadoop.bigdata.userdefineddatatypes.DesIntWritable;
import lombok.extern.slf4j.Slf4j;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
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
import java.util.List;

@Component
@Slf4j
public class WordCount {
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<Term> terms = ToAnalysis.parse(value.toString()).getTerms();
            for (Term i : terms) {
                if (!i.getName().equals("")) {
                    String nature = i.getNatureStr();
                    if (nature.equals("v") || nature.equals("n"))
                        context.write(new Text(i.getName()), new IntWritable(1));
                }
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

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
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);//设置reduce的个数
        FileInputFormat.addInputPath(job, new Path(name_node + in_path));
        FileOutputFormat.setOutputPath(job, new Path(name_node + out_path + "_d"));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(WordCountTopMapper.class);
        job2.setReducerClass(WordCountTopReducer.class);
        job2.setOutputKeyClass(DesIntWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);//设置reduce的个数
        FileInputFormat.addInputPath(job2, new Path(name_node + out_path + "_d"));
        FileOutputFormat.setOutputPath(job2, new Path(name_node + out_path));
        job2.waitForCompletion(true);
    }
}

