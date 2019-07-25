package cn.neu.hadoop.bigdata.mapreduce.pagerank;

import cn.neu.hadoop.bigdata.mapreduce.NameSplit;
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
    public static class PageRankIterMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] name_and_pr_relation_list = value.toString().split("\t");
            String[] pr_and_relation_list = name_and_pr_relation_list[1].split("#");
            float point = Float.valueOf(pr_and_relation_list[0]);

            for (String i : pr_and_relation_list[1].split(";")) {
                String[] relationship_name_and_weight = i.split(":");
                context.write(new Text(relationship_name_and_weight[0]), new Text(String.valueOf(point * Float.valueOf(relationship_name_and_weight[1]))));
            }
            context.write(new Text(name_and_pr_relation_list[0]), new Text('#' + pr_and_relation_list[1]));
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

    public static void main(String in_path, String out_path, int repeat_time, String name_node) throws IOException, ClassNotFoundException, InterruptedException {
        String tmp_output_path = "/test/tmp/pagerank/";
        int tmp_count = 0;

        GraphBuilder.main(name_node + in_path, name_node + tmp_output_path + tmp_count);
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
            if (repeat_time == 1) FileOutputFormat.setOutputPath(job, new Path(name_node + out_path));
            else FileOutputFormat.setOutputPath(job, new Path(name_node + tmp_output_path + tmp_count));
            job.waitForCompletion(true);
            repeat_time--;
            tmp_count++;
        }
        PageRankViewer.main(name_node + out_path, name_node + out_path + "_d");
    }
}
