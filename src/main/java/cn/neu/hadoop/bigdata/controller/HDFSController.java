package cn.neu.hadoop.bigdata.controller;

import cn.neu.hadoop.bigdata.bean.HDFSList.ListReturnInterface;
import cn.neu.hadoop.bigdata.service.impl.HDFSServiceImpl;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletResponse;

@Controller
@Slf4j
@RequestMapping("fs")
public class HDFSController {
    @Autowired
    HDFSServiceImpl hdfsService;

    @ResponseBody
    @RequestMapping(value = "list")
    public ListReturnInterface getList(@RequestParam(name = "path") String path) {
        return hdfsService.get_list(path);
    }

    @ResponseBody
    @RequestMapping(value = "upload")
    public String upload(@RequestBody JSONObject post_json) {
        return hdfsService.upload(post_json.getString("filepath"), post_json.getString("content"));
    }

    @ResponseBody
    @RequestMapping(value = "mk")
    public String mk(@RequestParam(name = "dir_name") String path) {
        return hdfsService.mk(path);
    }

    @ResponseBody
    @RequestMapping(value = "rm")
    public String rm(@RequestParam(name = "delete_list") String delete_list) {
        return hdfsService.rm(delete_list);
    }

    @RequestMapping(value = "download")
    public void download(HttpServletResponse response, @RequestParam(name = "download_filename") String download_filename_path) {
        hdfsService.download(response, download_filename_path);
    }
}