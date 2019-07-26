package cn.neu.hadoop.bigdata.bean.echarts.series.wordcloud;

public class EchartsWordCloudAData {
    private String name;
    private float value;
    private EchartsWordCloudItemStyle itemStyle;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public EchartsWordCloudItemStyle getItemStyle() {
        return itemStyle;
    }

    public void setItemStyle(EchartsWordCloudItemStyle itemStyle) {
        this.itemStyle = itemStyle;
    }
}
