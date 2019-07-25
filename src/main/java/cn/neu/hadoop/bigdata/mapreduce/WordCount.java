package cn.neu.hadoop.bigdata.mapreduce;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import com.hankcs.hanlp.tokenizer.SpeedTokenizer;
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
import java.util.List;

@Component
@Slf4j
public class WordCount {
    private static int word_time = 1;

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            List<Term> terms = SpeedTokenizer.segment(value.toString());
//            for (Term i : terms) {
//                if (!i.word.equals("")) {
//                    String nature = i.nature.toString();
//                    if (nature.equals("v") || nature.equals("n")) {
//                        int num;
//                        if (word_time <= 0)
//                            num = 1;
//                        else num = (int) Math.pow(i.word.length(), word_time);
//                        context.write(new Text(i.word), new IntWritable(num));
//                    }
//                }
//            }
            List<String> terms = HanLP.extractKeyword(value.toString(),2);
            for(String i:terms){
                if(!"".equals(i)) context.write(new Text(i), new IntWritable(1));
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
        FileOutputFormat.setOutputPath(job, new Path(name_node + out_path));
        job.waitForCompletion(true);
    }
}

