package cn.neu.hadoop.bigdata.bean.echarts.series.graph;

public class EchartsGraphNode {
    private String name;
    private int category;
    private float value;
    private float symbolSize;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCategory() {
        return category;
    }

    public void setCategory(int category) {
        this.category = category;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public float getSymbolSize() {
        return symbolSize;
    }

    public void setSymbolSize(float symbolSize) {
        this.symbolSize = symbolSize;
    }
}
