package cn.neu.hadoop.bigdata;

import lombok.extern.slf4j.Slf4j;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.recognition.impl.NatureRecognition;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.List;

@Configuration
@Slf4j
public class NameSplit {
    @Autowired
    FileSystem fileSystem;
    @Value("${hadoop.name-node}")
    private String nameNode;
    @Autowired
    HadoopTemplate hadoopTemplate;

    private String input_path = "/test/input";
    private String output_path = "/test/output1";

    @Bean("namesplit")
    public NameSplit get_namesplit() {
        return new NameSplit();
    }

    NameSplit() {
    }

    NameSplit(String in_path, String out_path) {
        this.input_path = in_path;
        this.output_path = out_path;
    }

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

    public void main() throws IOException, InterruptedException, ClassNotFoundException {
        //判断output文件夹是否存在，如果存在则删除
        Path in = new Path(nameNode + input_path);
        Path out = new Path(nameNode + output_path);
        assert fileSystem.exists(in);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);//true的意思是，就算output里面有东西，也一带删除
        }

        Job job = Job.getInstance();
        job.setJarByClass(NameSplit.class);
        job.setMapperClass(NameLineMapper.class);
        job.setReducerClass(NameLineReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);//设置reduce的个数
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.waitForCompletion(true);
    }
}
