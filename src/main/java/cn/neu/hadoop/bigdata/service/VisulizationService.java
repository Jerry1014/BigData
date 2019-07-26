package cn.neu.hadoop.bigdata.service;

import cn.neu.hadoop.bigdata.bean.echarts.EchartsOptionBar;
import cn.neu.hadoop.bigdata.bean.echarts.EchartsOptionGraph;
import cn.neu.hadoop.bigdata.bean.echarts.EchartsOptionWordcloud;

public interface VisulizationService {
    EchartsOptionBar get_echarts_bar_json(String filepath) throws Exception;

    EchartsOptionGraph get_echarts_graph_json(String filepath) throws Exception;

    EchartsOptionWordcloud get_echarts_wordcount_json(String filepath) throws Exception;
}
