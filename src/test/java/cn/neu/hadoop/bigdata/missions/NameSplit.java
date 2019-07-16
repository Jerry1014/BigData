package cn.neu.hadoop.bigdata.missions;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
@Slf4j
public class NameSplit {
    @Autowired
    FileSystem fileSystem;
    @Value("${hadoop.name-node}")
    private String nameNode;

    private String input_path = "/test/inputs";
    private String output_path = "/test/outputs";

    @Test
    public void name_split() throws IOException {
        //判断output文件夹是否存在，如果存在则删除
        Path in = new Path(nameNode + input_path);
        Path out = new Path(nameNode + output_path);
        assert fileSystem.exists(in);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);//true的意思是，就算output里面有东西，也一带删除
        }

        // todo 调用namesplit进行名字的分割
    }
}
