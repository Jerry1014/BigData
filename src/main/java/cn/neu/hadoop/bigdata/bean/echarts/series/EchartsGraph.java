package cn.neu.hadoop.bigdata.bean.echarts.series;

import cn.neu.hadoop.bigdata.bean.echarts.series.EchartsSeriesBase;
import cn.neu.hadoop.bigdata.bean.echarts.series.graph.EchartsGraphCategoryList;
import cn.neu.hadoop.bigdata.bean.echarts.series.graph.EchartsGraphLinkList;
import cn.neu.hadoop.bigdata.bean.echarts.series.graph.EchartsGraphNodeList;

public class EchartsGraph extends EchartsSeriesBase {
    private String layout;
    private boolean roam;
    private boolean focusNodeAdjacency;
    private EchartsGraphNodeList echartsGraphNodeList;
    private EchartsGraphLinkList echartsGraphLinkList;
    private EchartsGraphCategoryList echartsGraphCategoryList;

    public String getLayout() {
        return layout;
    }

    public void setLayout(String layout) {
        this.layout = layout;
    }

    public boolean isRoam() {
        return roam;
    }

    public void setRoam(boolean roam) {
        this.roam = roam;
    }

    public boolean isFocusNodeAdjacency() {
        return focusNodeAdjacency;
    }

    public void setFocusNodeAdjacency(boolean focusNodeAdjacency) {
        this.focusNodeAdjacency = focusNodeAdjacency;
    }

    public EchartsGraphNodeList getEchartsGraphNodeList() {
        return echartsGraphNodeList;
    }

    public void setEchartsGraphNodeList(EchartsGraphNodeList echartsGraphNodeList) {
        this.echartsGraphNodeList = echartsGraphNodeList;
    }

    public EchartsGraphLinkList getEchartsGraphLinkList() {
        return echartsGraphLinkList;
    }

    public void setEchartsGraphLinkList(EchartsGraphLinkList echartsGraphLinkList) {
        this.echartsGraphLinkList = echartsGraphLinkList;
    }

    public EchartsGraphCategoryList getEchartsGraphCategoryList() {
        return echartsGraphCategoryList;
    }

    public void setEchartsGraphCategoryList(EchartsGraphCategoryList echartsGraphCategoryList) {
        this.echartsGraphCategoryList = echartsGraphCategoryList;
    }
}
