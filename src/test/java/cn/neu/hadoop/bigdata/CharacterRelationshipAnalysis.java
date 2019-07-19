package cn.neu.hadoop.bigdata;

import cn.neu.hadoop.bigdata.LPA.LPACompute;
import cn.neu.hadoop.bigdata.pagerank.PageRankCompute;
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
    @Value("${hadoop.namespace}")
    private String namespace;
    @Autowired
    private HadoopTemplate hadoopTemplate;

    @Test
    public void all_mission_test() {
        // 任务初始化
        String source_all_person_name_file_path = "C:\\tem\\jinyong_all_person.txt";
        String all_person_name_file_path = namespace + "/userdic";
        String all_person_filename = "jinyong_all_person.txt";
        String local_source_data_path = "C:\\tem\\data";
        String hadoop_source_data_path = namespace + "/input";
        String name_split_output_path = namespace + "/output_ns";
        String name_count_output_path = namespace + "/output_nc";
        String relationship_count_output_path = namespace + "/output_rc";
        String build_relationship_output_path = namespace + "/output_br";
        String page_rank_output_path = namespace + "/output_pr";
        String lpa_output_path = namespace + "/output_lpa";
        String local_result_save_path = "C:\\tem\\";

        try {
            clear_output_directory(hadoop_source_data_path);
            hadoopTemplate.uploadDir(local_source_data_path, hadoop_source_data_path);

            clear_output_directory(name_split_output_path);
            hadoopTemplate.uploadFile(source_all_person_name_file_path, all_person_name_file_path);
            String all_person_name = hadoopTemplate.read(true, all_person_name_file_path + '/' + all_person_filename);
            NameSplit.main(hadoop_source_data_path, name_split_output_path, name_node, all_person_name);

            clear_output_directory(name_count_output_path);
            NameCount.main(name_split_output_path, name_count_output_path, name_node);

            clear_output_directory(build_relationship_output_path);
            BuildRelationshipMap.main(name_count_output_path, build_relationship_output_path, name_node);

            clear_output_directory(page_rank_output_path);
            clear_output_directory(namespace + "/tmp/pagerank");
            PageRankCompute.main(build_relationship_output_path, page_rank_output_path, 10, name_node);
            clear_output_directory(namespace + "/tmp/pagerank");

            clear_output_directory(lpa_output_path);
            clear_output_directory(namespace + "/tmp/lpa");
            LPACompute.main(page_rank_output_path, lpa_output_path, 6, name_node);
            clear_output_directory(namespace + "/tmp/lpa");

            hadoopTemplate.read(false, lpa_output_path + "/part-r-00000");
            hadoopTemplate.download(lpa_output_path + "/part-r-00000", local_result_save_path);
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
