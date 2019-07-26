package cn.neu.hadoop.bigdata.service.impl;

import cn.neu.hadoop.bigdata.service.InitOptions;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class InitOptionsImpl implements InitOptions {
    private String[] all_analysis = {"NameSplit", "RelationshipCount", "BuildRelationshipMap", "PageRankCompute", "LPACompute", "NameCount", "WordCount", "WordTop"};
    private String[] all_charts = {"WordCount", "Graph", "WordCloud"};

    @Override
    public Map<String, String[]> get_all_option() {
        Map<String, String[]> return_map = new HashMap<>();
        return_map.put("all_analysis", all_analysis);
        return_map.put("all_charts", all_charts);
        return return_map;
    }
}
