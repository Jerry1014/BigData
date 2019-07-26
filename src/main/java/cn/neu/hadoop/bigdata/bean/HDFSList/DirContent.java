package cn.neu.hadoop.bigdata.bean.HDFSList;

public class DirContent implements ListReturnInterface {
    private String status;
    private EachFileDirInfo[] content;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public EachFileDirInfo[] getContent() {
        return content;
    }

    public void setContent(EachFileDirInfo[] content) {
        this.content = content;
    }
}
