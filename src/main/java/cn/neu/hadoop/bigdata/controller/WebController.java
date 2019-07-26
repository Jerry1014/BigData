package cn.neu.hadoop.bigdata.controller;

import cn.neu.hadoop.bigdata.hadoop.HadoopTemplate;
import cn.neu.hadoop.bigdata.mapreduce.*;
import cn.neu.hadoop.bigdata.mapreduce.LPA.LPACompute;
import cn.neu.hadoop.bigdata.mapreduce.pagerank.PageRankCompute;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.util.HashSet;

@Controller
@Slf4j
public class WebController {
    @Autowired
    private HadoopTemplate hadoopTemplate;
    @Value("${hadoop.name-node}")
    private String name_node;
    private static String tem_file_save_path = "C:/tem/";
    private String[] all_analysis = {"NameSplit", "RelationshipCount", "BuildRelationshipMap", "PageRankCompute", "LPACompute", "NameCount", "WordCount", "WordTop"};
    private String[] all_charts = {"WordCount", "Graph", "WordCloud"};
    private String[] my_color = {"#c23531", "#2f4554", "#61a0a8", "#d48265", "#91c7ae", "#749f83", "#ca8622",
            "#bda29a", "#6e7074", "#546570", "#c4ccd3", "#FFFF00", "#FF83FA", "#D8BFD8", "#CDCDB4", "#CDBE70",
            "#CD2990", "#C1FFC1", "#C0FF3E", "#8B2500", "#8B008B", "#6C7B8B", "#3A5FCD", "#000000"};

    @RequestMapping(value = "/visualizing")
    public String visualizing(@RequestParam(name = "chart") String method) {
        return method.equals("WordCloud") ? "visualizing-wordcloud.html" : "visualizing.html";
    }

    @ResponseBody
    @RequestMapping(value = "/get_all_method")
    public String get_analysis_method() {
        JsonObject response_json = new JsonObject();
        JsonArray analysis_list = new JsonArray();
        for (String i : all_analysis) {
            analysis_list.add(i);
        }
        response_json.add("all_analysis", analysis_list);
        JsonArray charts_list = new JsonArray();
        for (String i : all_charts) {
            charts_list.add(i);
        }
        response_json.add("all_charts", charts_list);
        return response_json.toString();
    }

    @ResponseBody
    @RequestMapping(value = "/echarts_json")
    public String get_echarts_json(HttpServletResponse response, @RequestParam(name = "filepath") String filepath, @RequestParam(name = "chart") String method) {
        JsonObject response_json = new JsonObject();
        try {
            String filename = filepath.substring(filepath.lastIndexOf('/'));

            JsonObject title = new JsonObject();
            title.addProperty("text", filename + ' ' + method);
            response_json.add("title", title);
            JsonObject tooltip = new JsonObject();
            response_json.add("tooltip", tooltip);

            switch (method) {
                case "WordCount":
                    JsonArray dataZoom = new JsonArray();
                    JsonObject dataZoom_x_inside = new JsonObject();
                    dataZoom_x_inside.addProperty("type", "inside");
                    dataZoom_x_inside.addProperty("xAxisIndex", 0);
                    JsonObject dataZoom_y_inside = new JsonObject();
                    dataZoom_y_inside.addProperty("type", "inside");
                    dataZoom_y_inside.addProperty("yAxisIndex", 0);
                    dataZoom.add(dataZoom_y_inside);
                    dataZoom.add(dataZoom_x_inside);
                    response_json.add("dataZoom", dataZoom);
                    JsonObject yAxis = new JsonObject();
                    response_json.add("yAxis", yAxis);
                    JsonObject legend = new JsonObject();
                    JsonArray legend_data = new JsonArray();
                    legend_data.add("次数");
                    legend.add("data", legend_data);
                    response_json.add("legend", legend);

                    JsonArray xAxis_data = new JsonArray();
                    JsonArray series_data = new JsonArray();
                    String[] words = hadoopTemplate.read(true, filepath).split("\n");
                    for (String i : words) {
                        String[] name_count = i.split("\t");
                        xAxis_data.add(name_count[0]);
                        series_data.add(Integer.valueOf(name_count[1]));
                    }
                    JsonObject xAxis = new JsonObject();
                    xAxis.add("data", xAxis_data);
                    response_json.add("xAxis", xAxis);
                    JsonArray series_list = new JsonArray();
                    JsonObject series = new JsonObject();
                    series.addProperty("name", "次数");
                    series.addProperty("type", "bar");
                    series.add("data", series_data);
                    series_list.add(series);
                    response_json.add("series", series_list);
                    break;
                case "Graph":
                    JsonObject g_series = new JsonObject();
                    g_series.addProperty("layout", "force");
                    g_series.addProperty("type", "graph");
                    g_series.addProperty("roam", true);
                    g_series.addProperty("focusNodeAdjacency", true);
                    JsonArray g_nodes = new JsonArray();
                    JsonArray g_links = new JsonArray();
                    String[] g_words = hadoopTemplate.read(true, filepath).split("\n");
                    HashSet<Integer> all_category = new HashSet<>();
                    for (String i : g_words) {
                        String[] label_and_name_point_relationship = i.split("\t");
                        String[] name_and_point_and_relationship = label_and_name_point_relationship[1].split("#");
                        JsonObject tem_node = new JsonObject();
                        tem_node.addProperty("name", name_and_point_and_relationship[0]);
                        tem_node.addProperty("category", Integer.valueOf(label_and_name_point_relationship[0]));
                        all_category.add(Integer.valueOf(label_and_name_point_relationship[0]));
                        tem_node.addProperty("value", Float.valueOf(name_and_point_and_relationship[1]));
                        tem_node.addProperty("symbolSize", Float.valueOf(name_and_point_and_relationship[1]));
                        g_nodes.add(tem_node);
                        for (String j : name_and_point_and_relationship[2].split(";")) {
                            String[] target_name_and_weight = j.split(":");
                            JsonObject tem_link = new JsonObject();
                            tem_link.addProperty("source", name_and_point_and_relationship[0]);
                            tem_link.addProperty("target", target_name_and_weight[0]);
                            g_links.add(tem_link);
                        }
                    }
                    g_series.add("data", g_nodes);
                    g_series.add("links", g_links);
                    JsonArray categories = new JsonArray();
                    for (int i = 0; i < all_category.size(); i++) {
                        JsonObject tem_category = new JsonObject();
                        tem_category.addProperty("name", String.valueOf(i));
                        categories.add(tem_category);
                    }
                    g_series.add("categories", categories);
                    JsonArray g_all_series = new JsonArray();
                    g_all_series.add(g_series);
                    response_json.add("series", g_all_series);
                    JsonArray color = new JsonArray();
                    for (String i : my_color) {
                        color.add(i);
                    }
                    response_json.add("color", color);
                    break;
                case "WordCloud":
                    JsonObject w_series = new JsonObject();
                    w_series.addProperty("type", "wordCloud");
                    JsonArray text_rotation = new JsonArray();
                    text_rotation.add(-45);
                    text_rotation.add(0);
                    text_rotation.add(45);
                    text_rotation.add(90);
                    w_series.add("textRotation", text_rotation);
                    w_series.addProperty("textPadding", 0);
                    JsonObject auto_size = new JsonObject();
                    auto_size.addProperty("enable", true);
                    auto_size.addProperty("minSize", 10);
                    w_series.add("autoSize", auto_size);
                    JsonArray w_size = new JsonArray();
                    w_size.add("80%");
                    w_size.add("80%");
                    w_series.add("size", w_size);
                    JsonArray w_data = new JsonArray();
                    String[] w_words = hadoopTemplate.read(true, filepath).split("\n");
                    for (int j = 0; j < 300 && j < w_words.length; j++) {
                        String i = w_words[j];
                        String[] value_word = i.split("\t");
                        JsonObject tem_a_word = new JsonObject();
                        tem_a_word.addProperty("name", value_word[1]);
                        tem_a_word.addProperty("value", Float.valueOf(value_word[0]));
                        JsonObject w_itemStyle = new JsonObject();
                        JsonObject w_color = new JsonObject();
                        w_color.addProperty("color", String.format("rgb(%d,%d,%d)", Math.round(Math.random() * 255), Math.round(Math.random() * 255), Math.round(Math.random() * 255)));
                        w_itemStyle.add("normal", w_color);
                        tem_a_word.add("itemStyle", w_itemStyle);
                        w_data.add(tem_a_word);
                    }
                    w_series.add("data", w_data);
                    JsonArray w_all_series = new JsonArray();
                    w_all_series.add(w_series);
                    response_json.add("series", w_all_series);
                    break;
                default:
                    throw new Exception("无此可视化图表");
            }
        } catch (Exception e) {
            response.setStatus(400);
            e.printStackTrace();
        }
        return response_json.toString();
    }
}