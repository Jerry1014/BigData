package cn.neu.hadoop.bigdata.LPA;

import cn.neu.hadoop.bigdata.pagerank.GraphBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.stereotype.Component;

import java.io.IOException;


@Component
@Slf4j
/**
 * 接受PageRankCompute的输出
 * 输入格式为 一灯大师 PR#完颜萍:0.5;小龙女:0.5
 * 输出格式为 一灯大师#(int)1 PR#完颜萍:0.5;小龙女:0.5
 */
public class LPAInit {
    public static class InitMap extends Mapper<Object, Text, Text, Text> {
        private static int label_num = 0;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] key_value = value.toString().split("\t");
            context.write(new Text(key_value[0] + '#' + label_num), new Text(key_value[1]));
            label_num++;
        }
    }

    public static void main(String input_path, String output_path) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(LPAInit.class);
        job.setMapperClass(InitMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));
        job.waitForCompletion(true);

    }
}
