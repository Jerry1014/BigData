package cn.neu.hadoop.bigdata.bean.echarts;

import cn.neu.hadoop.bigdata.bean.echarts.common.EchartsDataZoom;
import cn.neu.hadoop.bigdata.bean.echarts.common.EchartsxAxis;
import cn.neu.hadoop.bigdata.bean.echarts.common.EchartsyAxis;

public class EchartsOptionBar extends EchartsOptionBase {
    private EchartsDataZoom[] dataZoom;
    private EchartsxAxis xAxis;
    private EchartsyAxis yAxis;

    public EchartsDataZoom[] getDataZoom() {
        return dataZoom;
    }

    public void setDataZoom(EchartsDataZoom[] dataZoom) {
        this.dataZoom = dataZoom;
    }

    public EchartsxAxis getxAxis() {
        return xAxis;
    }

    public void setxAxis(EchartsxAxis xAxis) {
        this.xAxis = xAxis;
    }

    public EchartsyAxis getyAxis() {
        return yAxis;
    }

    public void setyAxis(EchartsyAxis yAxis) {
        this.yAxis = yAxis;
    }
}
