package cn.neu.hadoop.bigdata.service.impl;

import cn.neu.hadoop.bigdata.bean.echarts.EchartsOptionBar;
import cn.neu.hadoop.bigdata.bean.echarts.EchartsOptionGraph;
import cn.neu.hadoop.bigdata.bean.echarts.EchartsOptionWordcloud;
import cn.neu.hadoop.bigdata.bean.echarts.common.*;
import cn.neu.hadoop.bigdata.bean.echarts.series.EchartsBar;
import cn.neu.hadoop.bigdata.bean.echarts.series.EchartsGraph;
import cn.neu.hadoop.bigdata.bean.echarts.series.EchartsSeriesBase;
import cn.neu.hadoop.bigdata.bean.echarts.series.graph.EchartsGraphCategory;
import cn.neu.hadoop.bigdata.bean.echarts.series.graph.EchartsGraphLink;
import cn.neu.hadoop.bigdata.bean.echarts.series.graph.EchartsGraphNode;
import cn.neu.hadoop.bigdata.hadoop.HadoopTemplate;
import cn.neu.hadoop.bigdata.service.VisulizationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

@Service
public class VisulizationServiceImpl implements VisulizationService {
    @Autowired
    HadoopTemplate hadoopTemplate;

    private String[] my_color = {"#c23531", "#2f4554", "#61a0a8", "#d48265", "#91c7ae", "#749f83", "#ca8622",
            "#bda29a", "#6e7074", "#546570", "#c4ccd3", "#FFFF00", "#FF83FA", "#D8BFD8", "#CDCDB4", "#CDBE70",
            "#CD2990", "#C1FFC1", "#C0FF3E", "#8B2500", "#8B008B", "#6C7B8B", "#3A5FCD", "#000000"};

    @Override
    public EchartsOptionBar get_echarts_bar_json(String filepath) throws Exception {
        EchartsOptionBar echartsOptionBar = new EchartsOptionBar();

        EchartsTitle tem_title = new EchartsTitle();
        tem_title.setText(filepath + " Bar");
        tem_title.setShow(true);
        echartsOptionBar.setTitle(tem_title);
        echartsOptionBar.setTooltip(new EchartsTooltip());

        EchartsDataZoomX tem_data_zoom_x = new EchartsDataZoomX();
        EchartsDataZoomY tem_data_zoom_y = new EchartsDataZoomY();
        tem_data_zoom_x.setType("inside");
        tem_data_zoom_x.setxAxisIndex(0);
        tem_data_zoom_y.setType("inside");
        tem_data_zoom_y.setyAxisIndex(0);
        EchartsDataZoom[] tem_data_zoom_list = {tem_data_zoom_x, tem_data_zoom_y};
        echartsOptionBar.setDataZoom(tem_data_zoom_list);

        String[] words = hadoopTemplate.read(true, filepath).split("\n");
        List<String> xAxis_data = new LinkedList<>();
        List<Float> series_data = new LinkedList<>();
        for (String i : words) {
            String[] name_count = i.split("\t");
            xAxis_data.add(name_count[0]);
            series_data.add(Float.valueOf(name_count[1]));
        }
        EchartsxAxis tem_xAxis = new EchartsxAxis();
        String[] tem_xAixs_data_list = new String[xAxis_data.size()];
        xAxis_data.toArray(tem_xAixs_data_list);
        tem_xAxis.setData(tem_xAixs_data_list);
        echartsOptionBar.setxAxis(tem_xAxis);
        echartsOptionBar.setyAxis(new EchartsyAxis());

        EchartsBar tem_series = new EchartsBar();
        Float[] test = new Float[series_data.size()];
        series_data.toArray(test);
        tem_series.setName("次数");
        tem_series.setData(test);

        EchartsSeriesBase[] tem_series_list = {tem_series};
        echartsOptionBar.setSeries(tem_series_list);
        return echartsOptionBar;
    }

    @Override
    public EchartsOptionGraph get_echarts_graph_json(String filepath) throws Exception {
        EchartsOptionGraph echartsOptionGraph = new EchartsOptionGraph();

        EchartsTitle echartsTitle = new EchartsTitle();
        echartsTitle.setText(filepath + " Grape");
        echartsTitle.setShow(true);
        echartsOptionGraph.setTitle(echartsTitle);

        EchartsGraph echartsGraph = new EchartsGraph();
        echartsGraph.setLayout("force");
        echartsGraph.setRoam(true);
        echartsGraph.setFocusNodeAdjacency(true);

        List<EchartsGraphNode> tem_node_list = new LinkedList<>();
        List<EchartsGraphLink> tem_link_list = new LinkedList<>();
        String[] g_words = hadoopTemplate.read(true, filepath).split("\n");
        HashSet<Integer> all_category = new HashSet<>();
        for (String i : g_words) {
            String[] label_and_name_point_relationship = i.split("\t");
            String[] name_and_point_and_relationship = label_and_name_point_relationship[1].split("#");
            EchartsGraphNode tem_node = new EchartsGraphNode();
            tem_node.setName(name_and_point_and_relationship[0]);
            int category = Integer.parseInt(label_and_name_point_relationship[0]);
            tem_node.setCategory(category);
            all_category.add(category);
            tem_node.setValue(Float.parseFloat(name_and_point_and_relationship[1]));
            tem_node_list.add(tem_node);
            for (String j : name_and_point_and_relationship[2].split(";")) {
                String[] target_name_and_weight = j.split(":");
                EchartsGraphLink tem_link = new EchartsGraphLink();
                tem_link.setSource(name_and_point_and_relationship[0]);
                tem_link.setTarget(target_name_and_weight[0]);
                tem_link_list.add(tem_link);
            }
        }
        echartsGraph.setNodes((EchartsGraphNode[]) tem_node_list.toArray());
        echartsGraph.setLinks((EchartsGraphLink[]) tem_link_list.toArray());

        List<EchartsGraphCategory> tem_category_list = new LinkedList<>();
        for (int i = 0; i < all_category.size(); i++) {
            EchartsGraphCategory tem_category = new EchartsGraphCategory();
            tem_category.setName(String.valueOf(i));
            tem_category_list.add(tem_category);
        }
        echartsGraph.setCategories((EchartsGraphCategory[]) tem_category_list.toArray());

        return null;
    }

    @Override
    public EchartsOptionWordcloud get_echarts_wordcount_json(String filepath) {
        return null;
    }
}
