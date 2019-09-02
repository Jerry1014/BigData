package cn.neu.hadoop.bigdata.bean.echarts.series;

import cn.neu.hadoop.bigdata.bean.echarts.series.pie.EchartsPieData;

public class EchartsPie extends EchartsSeriesBase {
    private String type = "pie";
    // private String roseType = "radius";
    private EchartsPieData[] data;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

//    public String getRoseType() {
//        return roseType;
//    }
//
//    public void setRoseType(String roseType) {
//        this.roseType = roseType;
//    }

    public EchartsPieData[] getData() {
        return data;
    }

    public void setData(EchartsPieData[] data) {
        this.data = data;
    }
}
