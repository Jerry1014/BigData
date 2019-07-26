package cn.neu.hadoop.bigdata.controller;

import cn.neu.hadoop.bigdata.service.impl.InitOptionsImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

@Controller
public class InitOptionsController {
    @Autowired
    InitOptionsImpl initOptions;

    @ResponseBody
    @RequestMapping(value = "/get_all_method")
    public Map<String, String[]> get_analysis_method() {
        return initOptions.get_all_option();
    }
}