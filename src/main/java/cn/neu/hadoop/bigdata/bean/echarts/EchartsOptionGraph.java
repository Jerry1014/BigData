package cn.neu.hadoop.bigdata.bean.echarts;

public class EchartsOptionGraph extends EchartsOptionBase {
    private String[] color = {"#c23531", "#2f4554", "#61a0a8", "#d48265", "#91c7ae", "#749f83", "#ca8622",
            "#bda29a", "#6e7074", "#546570", "#c4ccd3", "#FFFF00", "#FF83FA", "#D8BFD8", "#CDCDB4", "#CDBE70",
            "#CD2990", "#C1FFC1", "#C0FF3E", "#8B2500", "#8B008B", "#6C7B8B", "#3A5FCD", "#000000"};

    public String[] getColor() {
        return color;
    }

    public void setColor(String[] color) {
        this.color = color;
    }
}
