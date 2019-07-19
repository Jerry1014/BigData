package cn.neu.hadoop.bigdata.LPA;

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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


@Component
@Slf4j
public class LPACompute {
    // 对于要迭代的地方来说，不要瞎用static，并不会每次迭代创建新值
    public static class LPAIterMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] name_label_and_PR_relationship_list = value.toString().split("\t");
            String[] name_and_label = name_label_and_PR_relationship_list[0].split("#");
            String[] PR_and_relationship_list = name_label_and_PR_relationship_list[1].split("#");
            String[] relationship_name_and_weight_list = PR_and_relationship_list[1].split(";");
            float PR = Float.valueOf(PR_and_relationship_list[0]);

            // PR值和人物关系权重不作改变，使用$做不同map的区分
            context.write(new Text(name_and_label[0]), new Text('$' + name_label_and_PR_relationship_list[1]));

            // 输出 <链出人物名，人物名#人物标签#关系边权重>
            for (String i : relationship_name_and_weight_list) {
                String[] relationship_name_and_weight = i.split(":");
                context.write(new Text(relationship_name_and_weight[0]), new Text(name_and_label[0] + '#' +
                        name_and_label[1] + '#' + PR));
            }
        }
    }

    public static class LPAIterReduce extends Reducer<Text, Text, Text, Text> {
        HashMap<String, String> name_label = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> transmit_label_buffer = new LinkedList<>();
            String PR_name_list = "";
            for (Text i : values) {
                String tem = i.toString();
                if (tem.startsWith("$")) {
                    PR_name_list = tem.substring(1);
                } else transmit_label_buffer.add(tem);
            }

            HashMap<String, Float> relationship_name_weight = new HashMap<>();
            for (String i : PR_name_list.split("#")[1].split(";")) {
                relationship_name_weight.put(i.split(":")[0], Float.valueOf(i.split(":")[1]));
            }

            HashMap<String, Float> label_sum_weight = new HashMap<>();
            Float cur_max_weight = 0f;
            List<String> the_label_of_max_weight = new LinkedList<>();
            for (String i : transmit_label_buffer) {
                String[] relationship_name_and_transmit_label_and_transmit_pr = i.split("#");
                if (name_label.containsKey(relationship_name_and_transmit_label_and_transmit_pr[0]))
                    relationship_name_and_transmit_label_and_transmit_pr[1] = name_label.get(relationship_name_and_transmit_label_and_transmit_pr[0]);
                Float weight = relationship_name_weight.get(relationship_name_and_transmit_label_and_transmit_pr[0]);
//                Float weight = relationship_name_weight.get(relationship_name_and_transmit_label_and_transmit_pr[0]) * Float.valueOf(relationship_name_and_transmit_label_and_transmit_pr[2]);
                if (label_sum_weight.containsKey(relationship_name_and_transmit_label_and_transmit_pr[1]))
                    weight += label_sum_weight.get(relationship_name_and_transmit_label_and_transmit_pr[1]);
                label_sum_weight.put(relationship_name_and_transmit_label_and_transmit_pr[1], weight);
                if (weight > cur_max_weight) {
                    cur_max_weight = weight;
                    the_label_of_max_weight.clear();
                    the_label_of_max_weight.add(relationship_name_and_transmit_label_and_transmit_pr[1]);
                } else if (weight.equals(cur_max_weight)) {
                    the_label_of_max_weight.add(relationship_name_and_transmit_label_and_transmit_pr[1]);
                }
            }

            String update_label = the_label_of_max_weight.get((int) (Math.random() * the_label_of_max_weight.size()));
            name_label.put(key.toString(), update_label);
            context.write(new Text(key.toString() + '#' + update_label), new Text(PR_name_list));
        }
    }

    public static void main(String in_path, String out_path, int repeat_time, String name_node) throws InterruptedException, IOException, ClassNotFoundException {
        String tmp_output_path = "/test/tmp/lpa/";
        int tmp_count = 0;

        LPAInit.main(name_node + in_path, name_node + tmp_output_path + tmp_count);
        tmp_count++;
        while (repeat_time > 0) {
            Job job = Job.getInstance();
            job.setJarByClass(LPACompute.class);
            job.setMapperClass(LPAIterMapper.class);
            job.setReducerClass(LPAIterReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(1);//设置reduce的个数
            FileInputFormat.addInputPath(job, new Path(name_node + tmp_output_path + (tmp_count - 1)));
            FileOutputFormat.setOutputPath(job, new Path(name_node + tmp_output_path + tmp_count));
            job.waitForCompletion(true);

//            LPAResultClustering.main(name_node + tmp_output_path + tmp_count, name_node + tmp_output_path + tmp_count + "_d");
            repeat_time--;
            tmp_count++;
        }
        // 对lpa结果做聚类
        int num_cluster = LPAResultClustering.main(name_node + tmp_output_path + (tmp_count - 1), name_node + out_path);
        // 对聚类后的结果进行分离
        LPAReorganize.main(name_node + out_path, name_node + tmp_output_path + tmp_count, num_cluster);
    }
}
