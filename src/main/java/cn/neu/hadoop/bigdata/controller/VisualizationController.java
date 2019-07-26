package cn.neu.hadoop.bigdata.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class VisualizationController {
    @RequestMapping(value = "/visualizing")
    public String visualization(@RequestParam(name = "chart") String method) {
        return method.equals("WordCloud") ? "visualizing-wordcloud.html" : "visualizing.html";
    }
}
