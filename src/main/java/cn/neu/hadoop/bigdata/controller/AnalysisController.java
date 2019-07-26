package cn.neu.hadoop.bigdata.controller;

import cn.neu.hadoop.bigdata.service.impl.MapReduceServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

@Controller
public class AnalysisController {
    @Autowired
    MapReduceServiceImpl mapReduceService;

    @ResponseBody
    @RequestMapping(value = "/analysis")
    public Map<String, String> analysis(@RequestParam(name = "path") String path, @RequestParam(name = "method") String method) {
        return mapReduceService.analysis(path, method);
    }
}
