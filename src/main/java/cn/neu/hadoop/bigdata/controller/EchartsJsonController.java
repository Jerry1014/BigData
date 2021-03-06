package cn.neu.hadoop.bigdata.controller;

import cn.neu.hadoop.bigdata.bean.echarts.EchartsOptionBar;
import cn.neu.hadoop.bigdata.bean.echarts.EchartsOptionBase;
import cn.neu.hadoop.bigdata.service.VisulizationService;
import cn.neu.hadoop.bigdata.service.impl.VisulizationServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletResponse;

@Controller
@Slf4j
public class EchartsJsonController {
    @Autowired
    VisulizationServiceImpl visulizationService;

    @ResponseBody
    @RequestMapping(value = "/echarts_json")
    public EchartsOptionBase get_echarts_json(HttpServletResponse response, @RequestParam(name = "filepath") String filepath, @RequestParam(name = "chart") String method) {
        try {
            switch (method) {
                case "WordCount":
                    return visulizationService.get_echarts_bar_json(filepath);
                case "Graph":
                    return visulizationService.get_echarts_graph_json(filepath);
                case "WordCloud":
                    return visulizationService.get_echarts_wordcount_json(filepath);
                case "Pie":
                    return visulizationService.get_echarts_pie_json(filepath);
                default:
                    throw new Exception("无此可视化图表");
            }
        } catch (Exception e) {
            response.setStatus(400);
            log.error(e.getMessage());
        }
        return null;
    }
}
