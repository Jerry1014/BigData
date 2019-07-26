package cn.neu.hadoop.bigdata.service.impl;

import cn.neu.hadoop.bigdata.hadoop.HadoopTemplate;
import cn.neu.hadoop.bigdata.mapreduce.*;
import cn.neu.hadoop.bigdata.mapreduce.LPA.LPACompute;
import cn.neu.hadoop.bigdata.mapreduce.pagerank.PageRankCompute;
import cn.neu.hadoop.bigdata.service.MapReduceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class MapReduceServiceImpl implements MapReduceService {
    @Value("${hadoop.name-node}")
    private String name_node;
    @Autowired
    private HadoopTemplate hadoopTemplate;

    @Override
    public Map<String, String> analysis(String path, String method) {
        Map<String, String> return_map = new HashMap<>();
        try {
            switch (method) {
                case "NameSplit":
                    NameSplit.main(path, path.substring(0, path.lastIndexOf('/')) + "/output_ns", name_node);
                    break;
                case "WordCount":
                    WordCount.main(path, path.substring(0, path.lastIndexOf('/')) + "/output_wc", name_node);
                    break;
                case "NameCount":
                    NameCount.main(path, path.substring(0, path.lastIndexOf('/')) + "/output_nc", name_node);
                    break;
                case "RelationshipCount":
                    RelationshipCount.main(path, path.substring(0, path.lastIndexOf('/')) + "/output_rc", name_node);
                    break;
                case "BuildRelationshipMap":
                    BuildRelationshipMap.main(path, path.substring(0, path.lastIndexOf('/')) + "/output_br", name_node);
                    break;
                case "PageRankCompute":
                    if (hadoopTemplate.existsFile("/test/tmp/pagerank")) {
                        hadoopTemplate.delDir("/test/tmp/pagerank");//true的意思是，就算output里面有东西，也一带删除
                    }
                    PageRankCompute.main(path, path.substring(0, path.lastIndexOf('/')) + "/output_pr", 10, name_node);
                    break;
                case "LPACompute":
                    if (hadoopTemplate.existsFile("/test/tmp/lpa")) {
                        hadoopTemplate.delDir("/test/tmp/lpa");//true的意思是，就算output里面有东西，也一带删除
                    }
                    LPACompute.main(path, path.substring(0, path.lastIndexOf('/')) + "/output_lpa", 6, name_node);
                    break;
                case "WordTop":
                    WordTop.main(path, path.substring(0, path.lastIndexOf('/')) + "/output_wt", name_node);
                    break;
                default:
                    throw new Exception("无此方法");
            }
            return_map.put("status", "success");
        } catch (Exception e) {
            e.printStackTrace();
            return_map.put("status", "wrong");
            return_map.put("content", e.getMessage());
        }
        return return_map;
    }
}
