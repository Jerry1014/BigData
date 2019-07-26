package cn.neu.hadoop.bigdata.bean.echarts.common;

public class EchartsDataZoom {
    private int xAxisIndex;
    private int yAxisIndex;
    private String type;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getxAxisIndex() {
        return xAxisIndex;
    }

    public void setxAxisIndex(int xAxisIndex) {
        this.xAxisIndex = xAxisIndex;
    }

    public int getyAxisIndex() {
        return yAxisIndex;
    }

    public void setyAxisIndex(int yAxisIndex) {
        this.yAxisIndex = yAxisIndex;
    }
}

