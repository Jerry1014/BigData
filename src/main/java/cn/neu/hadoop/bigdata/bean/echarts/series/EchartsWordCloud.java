package cn.neu.hadoop.bigdata.bean.echarts.series;

import cn.neu.hadoop.bigdata.bean.echarts.series.wordcloud.EchartsWordCloudAData;
import cn.neu.hadoop.bigdata.bean.echarts.series.wordcloud.EchartsWordcloudAutoSize;

public class EchartsWordCloud extends EchartsSeriesBase {
    private String type = "wordCloud";
    private String[] size = {"80%", "%80"};
    private int[] textRotation = {0, 45, 90, -45};
    private EchartsWordcloudAutoSize autoSize;
    private EchartsWordCloudAData[] data;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String[] getSize() {
        return size;
    }

    public void setSize(String[] size) {
        this.size = size;
    }

    public int[] getTextRotation() {
        return textRotation;
    }

    public void setTextRotation(int[] textRotation) {
        this.textRotation = textRotation;
    }

    public EchartsWordcloudAutoSize getAutoSize() {
        return autoSize;
    }

    public void setAutoSize(EchartsWordcloudAutoSize autoSize) {
        this.autoSize = autoSize;
    }

    public EchartsWordCloudAData[] getData() {
        return data;
    }

    public void setData(EchartsWordCloudAData[] data) {
        this.data = data;
    }
}
