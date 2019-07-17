package cn.neu.hadoop.bigdata;

import lombok.extern.slf4j.Slf4j;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.recognition.impl.NatureRecognition;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
public class NameSplit {
    private static String input_path = "/test/input";
    private static String output_path = "/test/output1";

    public static class NameLineMapper extends Mapper<Object, Text, LongWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Result tem = ToAnalysis.parse(value.toString());
            new NatureRecognition().recognition(tem);
            List<Term> terms = tem.getTerms();

            for (Term i : terms) {
                if (i.getNatureStr().equals("nr")) {
                    context.write((LongWritable) key, new Text(i.getName()));
                }
            }
        }
    }

    public static class NameLineReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
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

    public static void main(String name_node) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(NameSplit.class);
        job.setMapperClass(NameLineMapper.class);
        job.setReducerClass(NameLineReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);//设置reduce的个数
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
