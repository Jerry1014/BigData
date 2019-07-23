package cn.neu.hadoop.bigdata.web;

import cn.neu.hadoop.bigdata.HadoopTemplate;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@Slf4j
public class WebController {
    @Autowired
    private HadoopTemplate hadoopTemplate;

    @RequestMapping(value = "/")
    public String index() {
        return "index.html";
    }

    @ResponseBody
    @RequestMapping(value = "/fs")
    public String get_json_from_hdfs(@RequestParam(name = "path") String path) {
        JsonObject response_json = new JsonObject();
        if (path != null) {
            try {
                boolean if_dir = hadoopTemplate.existDir(path, false);

                if (if_dir) {
                    if (!hadoopTemplate.existsFile(path)) throw new Exception("文件不存在");
                    JsonArray dir_filename_list = new JsonArray();
                    FileStatus[] test = hadoopTemplate.list(path);
                    for (FileStatus i : hadoopTemplate.list(path)) {
                        JsonObject tem = new JsonObject();
                        tem.addProperty("dir_or_file", i.isDirectory() ? "Dir" : "file");
                        String tem_path = i.getPath().toString();
                        tem.addProperty("path", tem_path.substring(tem_path.indexOf("/", 7)));
                        dir_filename_list.add(tem);
                    }
                    response_json.add("content", dir_filename_list);
                    response_json.addProperty("status", "dir");
                } else {
                    response_json.addProperty("content", hadoopTemplate.read(true, path));
                    response_json.addProperty("status", "file");
                }
            } catch (Exception e) {
                response_json.addProperty("status", "wrong");
                response_json.addProperty("content", e.toString());
            }
        } else {
            response_json.addProperty("status", "wrong");
            response_json.addProperty("content", "can't get parameter \"path\" or \"dir_or_file\"");
        }
        return response_json.toString();
    }

    @ResponseBody
    @RequestMapping(value = "/upload")
    public String upload(@RequestBody JSONObject post_json) {
        try {
            hadoopTemplate.write(post_json.getString("filepath"), post_json.getString("content"));
            return "文件上传成功";
        } catch (Exception e) {
            return "文件保存失败 " + e.toString();
        }
    }

    @ResponseBody
    @RequestMapping(value = "/mk")
    public String mk(@RequestParam(name = "dir_name") String path) {
        try {
            hadoopTemplate.existDir(path, true);
            return "文件夹创建成功";
        } catch (Exception e) {
            return "文件夹创建失败 " + e.toString();
        }
    }
}