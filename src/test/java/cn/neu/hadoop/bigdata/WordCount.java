package cn.neu.hadoop.bigdata;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
// 简单使用log
public class WordCount {
    @Autowired
    TokenizerMapper tokenizerMapper;
    @Autowired
    IntSumReducer intSumReducer;
    @Autowired
    FileSystem fileSystem;
    @Autowired
    HadoopTemplate hadoopTemplate;
    // 变量名必须为类名，首字母小写
    // 用于自动连接bean

    @Value("${hadoop.name-node}")
    private String nameNode;
    // 通过value获得application.properties中的定义


    private String inputPath = "/input";
    private String outputPath = "/output";

    @Test
    public void wordCount() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();

        //设置本次job作业使用的mapper类是那个
        job.setJarByClass(WordCount.class);
        //本次job作业使用的mapper类是那个？
        job.setMapperClass(TokenizerMapper.class);
        //本次job作业使用的reducer类是那个
        job.setCombinerClass(IntSumReducer.class);
        //本次job作业使用的reducer类是那个
        job.setReducerClass(IntSumReducer.class);
        //本次job作业使用的reducer类的输出数据key类型
        job.setOutputKeyClass(Text.class);
        //本次job作业使用的reducer类的输出数据value类型
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);//设置reduce的个数


        //判断output文件夹是否存在，如果存在则删除
        Path out = new Path(nameNode + outputPath);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);//true的意思是，就算output里面有东西，也一带删除
        }

        //本次job作业要处理的原始数据所在的路径
        FileInputFormat.addInputPath(job, new Path(nameNode + inputPath));
        //本次job作业产生的结果输出路径
        FileOutputFormat.setOutputPath(job, out);

        //提交本次作业
        job.waitForCompletion(true);
    }
}
