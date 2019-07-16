package cn.neu.hadoop.bigdata;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@Component
// 泛指各种组件
@ConditionalOnBean(FileSystem.class)
// 实例化到容器的必须条件
@Slf4j
// 日志框架
public class HadoopTemplate {
    @Autowired
    private FileSystem fileSystem;

    @Value("${hadoop.name-node}")
    private String nameNode;

    @Value("${hadoop.namespace:/}")
    private String nameSpace;

    @PostConstruct
    // 用于完成依赖注入项注入以执行任何初始化之后需要执行的方法，必须在类投入使用之前调用此方法
    public void init() {
        this.existDir(nameSpace, true);
    }

    public void uploadFile(String srcFile) {
        copyFileToHDFS(false, true, srcFile, nameSpace);
    }

    public void uploadFile(String srcFile, String destPath) {
        copyFileToHDFS(false, true, srcFile, destPath);
    }

    public void delFile(String fileName) {
        rmdir(nameSpace, fileName);
    }

    public void delDir(String path) {
        nameSpace = nameSpace + "/" + path;
        rmdir(path, null);
    }

    public void download(String fileName, String savePath) {
        getFile(nameSpace + "/" + fileName, savePath);
    }


    /**
     * 创建目录
     *
     * @param filePath 文件路径
     * @param create   当不存在时，是否创建
     * @return 文件是否存在标志
     */
    public boolean existDir(String filePath, boolean create) {
        boolean flag = false;
        if (StringUtils.isEmpty(filePath)) {
            throw new IllegalArgumentException("filePath不能为空");
        }
        try {
            Path path = new Path(filePath);
            if (create) {
                if (!fileSystem.exists(path)) {
                    fileSystem.mkdirs(path);
                }
            }
            if (fileSystem.isDirectory(path)) {
                flag = true;
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return flag;
    }


    /**
     * 文件上传至 HDFS
     *
     * @param delSrc    指是否删除源文件，true为删除，默认为false
     * @param overwrite 当文件已经存在时，是否覆写
     * @param srcFile   源文件，上传文件路径
     * @param destPath  hdfs的目的路径
     */
    public void copyFileToHDFS(boolean delSrc, boolean overwrite, String srcFile, String destPath) {
        // 源文件路径是Linux下的路径，如果在 windows 下测试，需要改写为Windows下的路径，比如D://hadoop/djt/weibo.txt
        Path srcPath = new Path(srcFile);

        // 目的路径
        if (StringUtils.isNotBlank(nameNode)) {
            destPath = nameNode + destPath;
        }
        Path dstPath = new Path(destPath);
        // 实现文件上传
        try {
            // 获取FileSystem对象
            fileSystem.copyFromLocalFile(srcPath, dstPath);
            fileSystem.copyFromLocalFile(delSrc, overwrite, srcPath, dstPath);
            //释放资源
            //    fileSystem.close();
        } catch (IOException e) {
            log.error("", e);
        }
    }


    /**
     * 删除文件或者文件目录
     *
     * @param path 路径
     */
    public void rmdir(String path, String fileName) {
        try {
            // 返回FileSystem对象
            if (StringUtils.isNotBlank(nameNode)) {
                path = nameNode + path;
            }
            if (StringUtils.isNotBlank(fileName)) {
                path = path + "/" + fileName;
            }
            // 删除文件或者文件目录  delete(Path f) 此方法已经弃用
            fileSystem.delete(new Path(path), true);
        } catch (IllegalArgumentException | IOException e) {
            log.error("", e);
        }
    }

    /**
     * 从 HDFS 下载文件
     *
     * @param hdfsFile hdfs文件路径
     * @param destPath 文件下载后,存放地址
     */
    public void getFile(String hdfsFile, String destPath) {
        // 源文件路径
        if (StringUtils.isNotBlank(nameNode)) {
            hdfsFile = nameNode + hdfsFile;
        }
        Path hdfsPath = new Path(hdfsFile);
        Path dstPath = new Path(destPath);
        try {
            // 下载hdfs上的文件
            fileSystem.copyToLocalFile(hdfsPath, dstPath);
            // 释放资源
            // fs.close();
        } catch (IOException e) {
            log.error("", e);
        }
    }

    public void writeSequenceFile(String hdfsFileName, Map<String, String> map) throws IOException {
        Text key = new Text();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fileSystem.getConf(), SequenceFile.Writer.file(new Path(hdfsFileName)),
                    SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(Text.class), SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
            for (String strKey : map.keySet()) {
                key.set(new Text(strKey));
                value.set(new Text(map.get(strKey)));
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    public Map<String, String> readSequenceFile(String hdfsFileName) {
        SequenceFile.Reader reader = null;
        Map<String, String> map = new HashMap<>();
        try {
            reader = new SequenceFile.Reader(fileSystem.getConf(), SequenceFile.Reader.file(new Path(hdfsFileName)));
            Text key = new Text();
            Text value = new Text();
            while (reader.next(key, value)) {
                map.put(key.toString(), value.toString());
                log.info(key.toString());
                log.info(value.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
        return map;
    }

    public void writeMapFile(String hdfsFileName, Map<String, String> map) throws IOException {
        Text key = new Text();
        Text value = new Text();
        MapFile.Writer writer = null;
        try {
            writer = new MapFile.Writer(fileSystem.getConf(), fileSystem, hdfsFileName,
                    key.getClass(), value.getClass(), SequenceFile.CompressionType.NONE);
            for (String strKey : map.keySet()) {
                key.set(strKey);
                value.set(map.get(strKey));
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    public Map<String, String> readMapFile(String hdfsFileName) {
        MapFile.Reader reader = null;
        Map<String, String> map = new HashMap<>();
        try {
            reader = new MapFile.Reader(fileSystem, hdfsFileName, fileSystem.getConf());
            Text value = new Text();
            WritableComparable key = (WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(), fileSystem.getConf());
            while (reader.next(key, value)) {
                log.info(key.toString() + ":" + value.toString());
                map.put(key.toString(), value.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
        return map;
    }

    /**
     * 读取hdfs文件
     *
     * @param fileName 要读取的hdfs文件路径
     * @return 文件内容str
     * @throws Exception 打开文件的IOException
     */
    public String read(String fileName) throws Exception {
        Path path = new Path(fileName);
        FSDataInputStream inStream = fileSystem.open(path);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            IOUtils.copyBytes(inStream, System.out, 4096, false);
            IOUtils.copyBytes(inStream, byteArrayOutputStream, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return byteArrayOutputStream.toString();
    }

    /**
     * 写入hdfs文件
     *
     * @param outputFileName 要写入的hdfs文件路径
     * @param content        要写入的内容
     * @throws Exception 打开文件的IOException
     */
    public void write(String outputFileName, String content) throws Exception {
        InputStream reader = new ByteArrayInputStream(content.getBytes());
        FSDataOutputStream outStream = fileSystem.create(new Path(outputFileName));
        try {
            IOUtils.copyBytes(reader, outStream, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
            IOUtils.closeStream(outStream);
        }
    }
}