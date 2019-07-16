package cn.neu.hadoop.bigdata;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

@Configuration
// 带有 @Configuration 的注解类表示这个类可以使用 Spring IoC 容器作为 bean 定义的来源。@Bean 注解告诉 Spring，一个带有
// @Bean 的注解方法将返回一个对象，该对象应该被注册为在 Spring 应用程序上下文中的 bean。
@ConditionalOnProperty(name="hadoop.name-node")
// 用于控制Configuration是否生效
@Slf4j
public class HadoopConfig {

    @Value(value = "${hadoop.name-node}")
    private String nameNode;

    @Bean("fileSystem")
    public FileSystem createFs(){
        //读取配置文件
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("dfs.replication", "1");
        // 文件系统
        FileSystem fs = null;
        // 返回指定的文件系统
        try {
            URI uri = new URI(nameNode.trim());
            fs = FileSystem.get(uri,conf);
            log.info("Success");
        } catch (Exception e) {
            log.error("", e);
        }
        return  fs;
    }
}