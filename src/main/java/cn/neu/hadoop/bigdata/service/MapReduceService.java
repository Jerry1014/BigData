package cn.neu.hadoop.bigdata.service;

import java.util.Map;

public interface MapReduceService {
    Map<String,String> analysis(String path, String method);
}
