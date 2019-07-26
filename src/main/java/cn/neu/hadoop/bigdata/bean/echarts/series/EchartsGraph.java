package cn.neu.hadoop.bigdata.bean.echarts.series;

import cn.neu.hadoop.bigdata.bean.echarts.series.graph.EchartsGraphCategory;
import cn.neu.hadoop.bigdata.bean.echarts.series.graph.EchartsGraphLink;
import cn.neu.hadoop.bigdata.bean.echarts.series.graph.EchartsGraphNode;

public class EchartsGraph extends EchartsSeriesBase {
    private String layout;
    private boolean roam;
    private boolean focusNodeAdjacency;
    private EchartsGraphNode[] nodes;
    private EchartsGraphLink[] links;
    private EchartsGraphCategory[] Categories;
    private String type = "graph";

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

    public EchartsGraphNode[] getNodes() {
        return nodes;
    }

    public void setNodes(EchartsGraphNode[] nodes) {
        this.nodes = nodes;
    }

    public EchartsGraphLink[] getLinks() {
        return links;
    }

    public void setLinks(EchartsGraphLink[] links) {
        this.links = links;
    }

    public EchartsGraphCategory[] getCategories() {
        return Categories;
    }

    public void setCategories(EchartsGraphCategory[] categories) {
        Categories = categories;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
