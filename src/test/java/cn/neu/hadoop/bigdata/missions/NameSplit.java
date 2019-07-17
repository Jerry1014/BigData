package cn.neu.hadoop.bigdata.missions;

import cn.neu.hadoop.bigdata.HadoopTemplate;
import cn.neu.hadoop.bigdata.namesplit.NameLineMapper;
import cn.neu.hadoop.bigdata.namesplit.NameLineReducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
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

    @Test
    public void name_split() throws IOException, InterruptedException, ClassNotFoundException {
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

        try {
            hadoopTemplate.read(output_path + "/part-r-00000");
        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }
    }
}
