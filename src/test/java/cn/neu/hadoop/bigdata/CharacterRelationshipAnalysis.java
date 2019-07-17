package cn.neu.hadoop.bigdata;

import lombok.extern.slf4j.Slf4j;
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
    @Value("${hadoop.name-node}")
    private String name_node;
    @Autowired
    private HadoopTemplate hadoopTemplate;
    @Value("${hadoop.namespace:/}")
    private String namespace;


    @Test
    public void all_mission_test() {
        // 任务初始化
        int cur_mission = 0;
        String source_data_path = "C:\\tem\\data";
        String input_path = namespace + "/input";
        String output_path_format = namespace + "/output";

        try {
            cur_mission = 4;

//            clear_output_directory(input_path);
//            hadoopTemplate.uploadDir(source_data_path, input_path);
//            cur_mission++;
//            clear_output_directory(output_path_format + cur_mission);
//            NameSplit.main(input_path, output_path_format + cur_mission, name_node);
//            cur_mission++;
//            clear_output_directory(output_path_format + cur_mission);
//            NameCount.main(output_path_format + (cur_mission - 1), output_path_format + cur_mission, name_node);
//            cur_mission++;
//            clear_output_directory(output_path_format + cur_mission);
//            BuildRelationshipMap.main(output_path_format + (cur_mission - 1), output_path_format + cur_mission, name_node);
//            cur_mission++;

            // 两种不同的人物关系计算方法
//            clear_output_directory(output_path_format + cur_mission);
//            clear_output_directory(namespace + "/tmp");
//            PageRankCompute.main(output_path_format + (cur_mission - 1), output_path_format + cur_mission, 1, name_node);
            cur_mission++;
            hadoopTemplate.read(output_path_format + (cur_mission - 1) + "/part-r-00000");
        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }
    }

    public void clear_output_directory(String path) throws IOException {
        if (hadoopTemplate.exists(path)) {
            hadoopTemplate.delDir(path);//true的意思是，就算output里面有东西，也一带删除
        }
    }
}
