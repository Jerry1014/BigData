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
    @Autowired
    private HadoopTemplate hadoopTemplate;
    // 不知道为什么，如果在配置文件中读取在下列拼接中会为null
    String namespace = "/test";

    private String source_all_person_name_file_path = "C:\\tem\\jinyong_all_person.txt";
    private String all_person_name_file_path = namespace + "/userdic/";
    private String all_person_filename = "jinyong_all_person.txt";
    private String local_source_data_path = "C:\\tem\\data";
    private String hadoop_source_data_path = namespace + "/input";
    private String name_split_output_path = namespace + "/output_ns";
    private String name_count_output_path = namespace + "/output_nc";
    private String relationship_count_output_path = namespace + "/output_rc";
    private String build_relationship_output_path = namespace + "/output_br";
    private String page_rank_output_path = namespace + "/output_pr";
    private String lpa_output_path = namespace + "/output_lpa";
    private String local_result_save_path = "C:\\tem\\";

    @Test
    public void all_mission_test() throws Exception {
        clear_output_directory(hadoop_source_data_path);
        hadoopTemplate.uploadDir(local_source_data_path, hadoop_source_data_path);

        clear_output_directory(name_split_output_path);
        hadoopTemplate.uploadFile(source_all_person_name_file_path, all_person_name_file_path + all_person_filename);
        String all_person_name = hadoopTemplate.read(true, all_person_name_file_path + all_person_filename);
        NameSplit.main(hadoop_source_data_path, name_split_output_path, name_node, all_person_name);

        clear_output_directory(name_count_output_path);
        NameCount.main(name_split_output_path, name_count_output_path, name_node);

        clear_output_directory(relationship_count_output_path);
        RelationshipCount.main(name_split_output_path, relationship_count_output_path, name_node);

        clear_output_directory(build_relationship_output_path);
        BuildRelationshipMap.main(relationship_count_output_path, build_relationship_output_path, name_node);

        clear_output_directory(page_rank_output_path);
        clear_output_directory(namespace + "/tmp/pagerank");
        PageRankCompute.main(build_relationship_output_path, page_rank_output_path, 10, name_node);
        clear_output_directory(namespace + "/tmp/pagerank");

        clear_output_directory(lpa_output_path);
        clear_output_directory(namespace + "/tmp/lpa");
        LPACompute.main(page_rank_output_path, lpa_output_path, 6, name_node);
        clear_output_directory(namespace + "/tmp/lpa");
        print_and_download_result(lpa_output_path);
    }

    @Test
    public void name_split() throws Exception {
        clear_output_directory(name_split_output_path);
        String all_person_name = hadoopTemplate.read(true, all_person_name_file_path + '/' + all_person_filename);
        NameSplit.main(hadoop_source_data_path, name_split_output_path, name_node, all_person_name);
        print_and_download_result(name_split_output_path);
    }

    @Test
    public void name_count() throws Exception {
        clear_output_directory(name_count_output_path);
        NameCount.main(name_split_output_path, name_count_output_path, name_node);
        print_and_download_result(name_count_output_path);
    }

    @Test
    public void relationship_count() throws Exception {
        clear_output_directory(relationship_count_output_path);
        RelationshipCount.main(name_split_output_path, relationship_count_output_path, name_node);
        print_and_download_result(relationship_count_output_path);
    }

    @Test
    public void build_relationship_map() throws Exception {
        clear_output_directory(build_relationship_output_path);
        BuildRelationshipMap.main(relationship_count_output_path, build_relationship_output_path, name_node);
        print_and_download_result(build_relationship_output_path);
    }

    @Test
    public void page_rank() throws Exception {
        clear_output_directory(page_rank_output_path);
        clear_output_directory(namespace + "/tmp/pagerank");
        PageRankCompute.main(build_relationship_output_path, page_rank_output_path, 10, name_node);
        clear_output_directory(namespace + "/tmp/pagerank");
        print_and_download_result(page_rank_output_path);
    }

    @Test
    public void lpa() throws Exception {
        clear_output_directory(lpa_output_path);
        clear_output_directory(namespace + "/tmp/lpa");
        LPACompute.main(page_rank_output_path, lpa_output_path, 6, name_node);
        clear_output_directory(namespace + "/tmp/lpa");
        print_and_download_result(lpa_output_path);
    }

    public void clear_output_directory(String path) throws IOException {
        if (hadoopTemplate.exists(path)) {
            hadoopTemplate.delDir(path);//true的意思是，就算output里面有东西，也一带删除
        }
    }

    public void print_and_download_result(String path) throws Exception {
        hadoopTemplate.read(false, path + "/part-r-00000");
        hadoopTemplate.download(path + "/part-r-00000", local_result_save_path);
    }
}
