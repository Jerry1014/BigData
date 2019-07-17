package cn.neu.hadoop.bigdata.missions;

import cn.neu.hadoop.bigdata.HadoopTemplate;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
@Slf4j
public class MissionInit {
    @Autowired
    private HadoopTemplate hadoopTemplate;

    @Test
    public void uploadFile() {
        hadoopTemplate.uploadDir("./data/金庸/", "/test/input");
    }
 }
