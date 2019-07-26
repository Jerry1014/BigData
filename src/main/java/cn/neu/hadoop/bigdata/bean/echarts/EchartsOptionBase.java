package cn.neu.hadoop.bigdata.bean.echarts;

import cn.neu.hadoop.bigdata.bean.echarts.common.EchartsTitle;
import cn.neu.hadoop.bigdata.bean.echarts.series.EchartsSeriesBase;

public class EchartsOptionBase {
    private EchartsTitle Title;
    private String Tooltip = "";
    private EchartsSeriesBase[] Series;

    public EchartsTitle getTitle() {
        return Title;
    }

    public void setTitle(EchartsTitle title) {
        Title = title;
    }

    public String getTooltip() {
        return Tooltip;
    }

    public void setTooltip(String tooltip) {
        Tooltip = tooltip;
    }

    public EchartsSeriesBase[] getSeries() {
        return Series;
    }

    public void setSeries(EchartsSeriesBase[] series) {
        Series = series;
    }
}
