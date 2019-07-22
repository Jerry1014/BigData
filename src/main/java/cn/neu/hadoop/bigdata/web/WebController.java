package cn.neu.hadoop.bigdata.web;

//import tem.HadoopTemplate;
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
//    @Autowired
//    private HadoopTemplate hadoopTemplate;

//    @ResponseBody
//    @RequestMapping(value = "/fs")
//    public String check(@RequestParam(name = "path")String path,@RequestParam(name = "dir_or_file")String dir_or_file){
//        JsonObject response_json = new JsonObject();
//        if (path != null && dir_or_file != null) {
//            try {
//                if (dir_or_file.equals("dir")) {
//                    JsonArray dir_filename_list = new JsonArray();
//                    hadoopTemplate.existsFile(path);
//                    FileStatus[] test = hadoopTemplate.list(path);
//                    for (FileStatus i : hadoopTemplate.list(path)) {
//                        JsonObject tem = new JsonObject();
//                        tem.addProperty("dir_or_file", i.isDirectory() ? "Dir" : "file");
//                        tem.addProperty("path", i.getPath().toString());
//                        dir_filename_list.add(tem);
//                    }
//                    response_json.add("dir_filename_list", dir_filename_list);
//                    response_json.addProperty("status", "dir");
//                } else {
//                    response_json.addProperty("file_content", hadoopTemplate.read(true, path));
//                    response_json.addProperty("status", "file");
//                }
//            } catch (Exception e) {
//                response_json.addProperty("status", "wrong");
//                response_json.addProperty("reason", e.toString());
//            }
//        } else {
//            response_json.addProperty("status", "wrong");
//            response_json.addProperty("reason", "can't get parameter \"path\" or \"dir_or_file\"");
//        }
//        return response_json.toString();
//    }

    @RequestMapping(value = "/")
    public String hello(){
        return "index.html";
    }

}