package cn.neu.hadoop.bigdata;

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
public class CharacterRelationshipAnalysis {
    @Autowired
    private FileSystem fileSystem;
    @Value("${hadoop.namespace:/}")
    private String namespace;
    @Value("${hadoop.name-node}")
    private String name_node;


    @Test
    public void all_mission_test() {
        // 任务初始化
        int cur_mission = 0;
        String source_data_path = "C:\\tem\\data";
        String input_path = name_node + namespace + "/input";
        String output_path_format = name_node + namespace + "/output";

        try {
            //clear_output_directory(input_path);
            //hadoopTemplate.uploadDir(source_data_path, input_path);
            cur_mission++;
            clear_output_directory(output_path_format + cur_mission);
            NameSplit.main(input_path, output_path_format + cur_mission);
            cur_mission++;
            clear_output_directory(output_path_format + cur_mission);
            NameCount.main(output_path_format + (cur_mission - 1), output_path_format + cur_mission);
            cur_mission++;
        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }
    }

    public void clear_output_directory(String path) throws IOException {
        Path out = new Path(path);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);//true的意思是，就算output里面有东西，也一带删除
        }
    }
}
