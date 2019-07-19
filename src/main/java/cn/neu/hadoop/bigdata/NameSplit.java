package cn.neu.hadoop.bigdata;

import lombok.extern.slf4j.Slf4j;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

@Component
@Slf4j
public class NameSplit {
    private static HashSet<String> name_set = new HashSet<>();

    public static class NameLineMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Result tem = ToAnalysis.parse(value.toString());
            new NatureRecognition().recognition(tem);
            List<Term> terms = tem.getTerms();

            for (Term i : terms) {
                if (i.getNatureStr().equals("nr")) {
                    if (name_set.contains(i.getName())) {
                        long line_num = ((LongWritable) key).get();
                        context.write(new Text(((FileSplit) context.getInputSplit()).getPath().getName()
                                + line_num), new Text(i.getName()));
                    }
                }
            }
        }
    }

    public static class NameLineReducer extends Reducer<Text, Text, NullWritable, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder name_list = new StringBuilder();
            for (Text i : values) {
                name_list.append(i.toString());
                name_list.append(' ');
            }
            context.write(NullWritable.get(), new Text(name_list.toString()));
        }
    }

    public static void main(String in_path, String out_path, String name_node, String all_person_name) throws IOException, InterruptedException, ClassNotFoundException {
        // 构建所有人物的字典
        name_set.clear();
        for (String i : all_person_name.split("\r\n")) {
            name_set.add(i);
            DicLibrary.insert(DicLibrary.DEFAULT, i, "nr", DicLibrary.DEFAULT_FREQ);
        }

        Job job = Job.getInstance();
        job.setJarByClass(NameSplit.class);
        job.setMapperClass(NameLineMapper.class);
        job.setReducerClass(NameLineReducer.class);
        // 哭了，这有个大坑，map的输出未指定是，会根据上一个即inputform的来，所以出现了LongWrite不匹配的问题
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);//设置reduce的个数
        FileInputFormat.addInputPath(job, new Path(name_node + in_path));
        FileOutputFormat.setOutputPath(job, new Path(name_node + out_path));
        job.waitForCompletion(true);
    }
}
