package cn.neu.hadoop.bigdata.service;

import cn.neu.hadoop.bigdata.bean.HDFSList.ListReturnInterface;

import javax.servlet.http.HttpServletResponse;

public interface HDFSService {
    ListReturnInterface get_list(String path);

    String upload(String filepath, String content);

    String mk(String path);

    String rm(String delete_list);

    void download(HttpServletResponse response,String filename);
}
