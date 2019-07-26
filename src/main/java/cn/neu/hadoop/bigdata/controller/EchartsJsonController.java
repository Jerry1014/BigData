package cn.neu.hadoop.bigdata.controller;

import cn.neu.hadoop.bigdata.bean.echarts.EchartsOptionBar;
import cn.neu.hadoop.bigdata.bean.echarts.EchartsOptionBase;
import cn.neu.hadoop.bigdata.service.VisulizationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletResponse;

@Controller
public class EchartsJsonController {
    @Autowired
    VisulizationService visulizationService;

    @ResponseBody
    @RequestMapping(value = "/test")
    public EchartsOptionBase get_echarts_json(HttpServletResponse response, @RequestParam(name = "filepath") String filepath, @RequestParam(name = "chart") String method) {
        try {
            switch (method) {
                case "WordCount":
                    return visulizationService.get_echarts_bar_json(filepath, method);
                case "Graph":
                    break;
                case "WordCloud":
                    break;
                default:
                    throw new Exception("无此可视化图表");
            }
        } catch (Exception e) {
            response.setStatus(400);
            e.printStackTrace();
        }
        return null;
    }
}