package cn.neu.hadoop.bigdata.pagerank;

import cn.neu.hadoop.bigdata.NameSplit;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
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
public class PageRankCompute {
    private static String input_path = "/test/input";
    private static String output_path = "/test/output4";
    private static String tmp_output_path = "/test/tmp/pagerank/";
    private static int tmp_count = 0;

    public static class PageRankIterMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] key_and_value = value.toString().split("\t");
            String[] point_and_relation_list = key_and_value[1].split("#");
            float point = Float.valueOf(point_and_relation_list[0]);

            for (String i : point_and_relation_list[1].split(";")) {
                String[] name_and_point = i.split(":");
                context.write(new Text(name_and_point[0]), new Text(String.valueOf(point * Float.valueOf(name_and_point[1]))));
            }
            context.write(new Text(key_and_value[0]), new Text('#' + point_and_relation_list[1]));
        }
    }

    public static class PageRankIterReduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String relationship_list = "";
            float sum = 0;
            for (Text i : values) {
                String tem = i.toString();
                if (tem.startsWith("#")) {
                    relationship_list = tem;
                } else {
                    sum += Float.valueOf(tem);
                }
            }
            context.write(key, new Text(sum + relationship_list));
        }
    }

    public static void main(int repeat_time, String name_node) throws IOException, ClassNotFoundException, InterruptedException {
        GraphBuilder.main(name_node + input_path, name_node + tmp_output_path + tmp_count);
        tmp_count++;
        while (repeat_time > 0) {
            Job job = Job.getInstance();
            job.setJarByClass(NameSplit.class);
            job.setMapperClass(PageRankIterMapper.class);
            job.setReducerClass(PageRankIterReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(1);//设置reduce的个数
            FileInputFormat.addInputPath(job, new Path(name_node + tmp_output_path + (tmp_count - 1)));
            FileOutputFormat.setOutputPath(job, new Path(name_node + tmp_output_path + tmp_count));
            job.waitForCompletion(true);
            repeat_time--;
            tmp_count++;
        }
        PageRankViewer.main(name_node + tmp_output_path + (tmp_count - 1), name_node + output_path);
    }

    public static void main(String in_path, String out_path, int repeat_time, String name_node) throws IOException, InterruptedException, ClassNotFoundException {
        input_path = in_path;
        output_path = out_path;
        main(repeat_time, name_node);
    }
}
