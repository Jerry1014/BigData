package cn.neu.hadoop.bigdata.bean.echarts.series;

public class EchartsBar extends EchartsSeriesBase{
    private String name;
    private Float[] data;
    private String type = "bar";

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public Float[] getData() {
        return data;
    }

    public void setData(Float[] data) {
        this.data = data;
    }
}
