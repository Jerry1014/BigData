package cn.neu.hadoop.bigdata.service.impl;

import cn.neu.hadoop.bigdata.bean.echarts.EchartsOptionBar;
import cn.neu.hadoop.bigdata.bean.echarts.EchartsOptionGraph;
import cn.neu.hadoop.bigdata.bean.echarts.EchartsOptionWordcloud;
import cn.neu.hadoop.bigdata.bean.echarts.common.EchartsDataZoom;
import cn.neu.hadoop.bigdata.bean.echarts.common.EchartsTitle;
import cn.neu.hadoop.bigdata.bean.echarts.common.EchartsxAxis;
import cn.neu.hadoop.bigdata.bean.echarts.common.EchartsyAxis;
import cn.neu.hadoop.bigdata.bean.echarts.series.EchartsBar;
import cn.neu.hadoop.bigdata.bean.echarts.series.EchartsSeriesBase;
import cn.neu.hadoop.bigdata.hadoop.HadoopTemplate;
import cn.neu.hadoop.bigdata.service.VisulizationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.List;

@Service
public class VisulizationServiceImpl implements VisulizationService {
    @Autowired
    HadoopTemplate hadoopTemplate;

    @Override
    public EchartsOptionBar get_echarts_bar_json(String filepath, String method) throws Exception {
        EchartsOptionBar echartsOptionBar = new EchartsOptionBar();

        EchartsTitle tem_title = new EchartsTitle();
        tem_title.setText(filepath + " Bar");
        echartsOptionBar.setTitle(tem_title);

        EchartsDataZoom tem_data_zoom = new EchartsDataZoom();
        tem_data_zoom.setType("inside");
        tem_data_zoom.setxAxisIndex(0);
        tem_data_zoom.setyAxisIndex(0);
        EchartsDataZoom[] tem_data_zoom_list = {tem_data_zoom};
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
    public EchartsOptionGraph get_echarts_graph_json(String filepath, String method) {
        return null;
    }

    @Override
    public EchartsOptionWordcloud get_echarts_wordcount_json(String filepath, String method) {
        return null;
    }
}
