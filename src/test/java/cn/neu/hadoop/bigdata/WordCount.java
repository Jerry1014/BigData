package cn.neu.hadoop.bigdata;

import cn.neu.hadoop.bigdata.wordcount.IntSumReducer;
import cn.neu.hadoop.bigdata.wordcount.TokenizerMapper;
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

    private String inputPath = "/test/word_count_test_input";
    private String outputPath = "/test/word_count_test_output";

    @Test
    public void wordCount() throws IOException, ClassNotFoundException, InterruptedException {
        //判断output文件夹是否存在，如果存在则删除
        Path out = new Path(nameNode + outputPath);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);//true的意思是，就算output里面有东西，也一带删除
        }
        Path in = new Path(nameNode + inputPath);
        if (fileSystem.exists(in)) {
            fileSystem.delete(in, true);//true的意思是，就算output里面有东西，也一带删除
        }

        try {
            hadoopTemplate.write(inputPath, "In the ways" +
                    " of Providence, there is always fitness in the smallest as in the greatest things. It is " +
                    "on the Fourth of July, in midsummer, that we hold the anniversary festivals of American " +
                    "Independence. And it is a beautiful ordering of the Providence that rules the seasons and " +
                    "the nations, that the time of these anniversaries is so well suited to the occasion. For it" +
                    " is fitting, that in the midst of glorious summer days, when the earth lies richest in the" +
                    " sunlight; when the fields are golden with the harvests; when the air is fragrant with the" +
                    " scent of flowers and the new hay; when, in a word, the beauty and the bounty of nature, " +
                    "unite to fill the heart with gladness and with gratitude, we should meet in kindred joy " +
                    "and thankfulness to celebrate our nation’s natal day. For sunshine is the symbol of " +
                    "prosperity, and summer the symbol of peace; and the wondrous bounty of the season fitly " +
                    "typifies the fruits of that civil and religious liberty, to establish which our fathers " +
                    "pledged their lives, their fortunes and their sacred honour. Not that all these " +
                    "anniversaries have been, or will be days of jubilee. Not that the chill and sombreness of " +
                    "winter have not settled, will not settle, upon some. For many stormy years were passed, " +
                    "before the hope that dawned on that July morning in ’76 became a full and crowned reality. ");
        } catch (Exception e) {
            log.error("", e);
        }

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

        //本次job作业要处理的原始数据所在的路径
        FileInputFormat.addInputPath(job, in);
        //本次job作业产生的结果输出路径
        FileOutputFormat.setOutputPath(job, out);

        //提交本次作业
        job.waitForCompletion(true);

        try {
            hadoopTemplate.read(outputPath + "/part-r-00000");
        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }
    }
}