package cn.neu.hadoop.bigdata;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
@Slf4j
public class HadoopTemplateTest {
    @Autowired
    private HadoopTemplate hadoopTemplate;


    @Test
    public void uploadFile() {
        hadoopTemplate.uploadFile("C:\\test.txt", "/test/");
    }

    @Test
    public void uploadFile1() {

    }

    @Test
    public void uploadDir(){
        hadoopTemplate.uploadDir("./data/金庸/", "/test/input");
        assert hadoopTemplate.existDir("/test/input", false);
    }

    @Test
    public void delFile() {
    }

    @Test
    public void delDir() {
    }

    @Test
    public void download() {
    }

    @Test
    public void existDir() {
        hadoopTemplate.init();
        log.info("" + hadoopTemplate.existDir("/test/333", true));
    }

    @Test
    public void copyFileToHDFS() {
    }

    @Test
    public void rmdir() {
    }

    @Test
    public void getFile() {
    }

    @Test
    public void testWriteSequenceFile() throws IOException {
        Map<String, String> map = new HashMap<>();
        map.put("name", "zhangsan");
        map.put("class", "dayi");
        hadoopTemplate.writeSequenceFile("/test/111.txt", map);
    }

    @Test
    public void readSequenceFile() {
        hadoopTemplate.readSequenceFile("/test/111.txt");
    }

    @Test
    public void testWriteMapFile() throws IOException {
        Map<String, String> map = new HashMap<>();
        map.put("class1", "dayi");
        map.put("name1", "zhangsan");

        hadoopTemplate.writeMapFile("/test", map);
    }

    @Test
    public void readMapFile() {
        hadoopTemplate.readMapFile("/test");
    }

    @Test
    public void read() throws Exception {
        hadoopTemplate.read("/test/555.txt");
    }

    @Test
    public void write() throws Exception {
        hadoopTemplate.write("/test/555.txt", "alskdfjlasdjf;lasdkjfl");
    }
}