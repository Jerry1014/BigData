package cn.neu.hadoop.bigdata.web;

import cn.neu.hadoop.bigdata.HadoopTemplate;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class WebController {
    @Autowired
    private HadoopTemplate hadoopTemplate;

    @ResponseBody
    @RequestMapping(value = "/fs")
    public String check(@RequestParam(name = "path") String path) {
        JsonObject response_json = new JsonObject();
        if (path != null) {
            try {
                boolean if_dir = hadoopTemplate.existDir(path, false);

                if (if_dir) {
                    if(! hadoopTemplate.existsFile(path)) throw new Exception("文件不存在");
                    JsonArray dir_filename_list = new JsonArray();
                    FileStatus[] test = hadoopTemplate.list(path);
                    for (FileStatus i : hadoopTemplate.list(path)) {
                        JsonObject tem = new JsonObject();
                        tem.addProperty("dir_or_file", i.isDirectory() ? "Dir" : "file");
                        tem.addProperty("path", i.getPath().toString());
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

    @RequestMapping(value = "/")
    public String hello() {
        return "index.html";
    }

}