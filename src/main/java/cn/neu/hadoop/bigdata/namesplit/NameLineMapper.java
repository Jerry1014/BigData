package cn.neu.hadoop.bigdata.namesplit;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.recognition.impl.NatureRecognition;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component
public class NameLineMapper extends Mapper<Object, Text, LongWritable, Text> {
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