package cn.neu.hadoop.bigdata.bean.echarts.series.wordcloud;

public class EchartsWordcloudAutoSize {
    private boolean enable = true;
    private int minSize = 10;

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public int getMinSize() {
        return minSize;
    }

    public void setMinSize(int minSize) {
        this.minSize = minSize;
    }
}
