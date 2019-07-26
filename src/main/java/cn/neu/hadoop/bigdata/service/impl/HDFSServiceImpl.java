package cn.neu.hadoop.bigdata.service.impl;

import cn.neu.hadoop.bigdata.bean.HDFSList.DirContent;
import cn.neu.hadoop.bigdata.bean.HDFSList.EachFileDirInfo;
import cn.neu.hadoop.bigdata.bean.HDFSList.FileContent;
import cn.neu.hadoop.bigdata.bean.HDFSList.ListReturnInterface;
import cn.neu.hadoop.bigdata.hadoop.HadoopTemplate;
import cn.neu.hadoop.bigdata.service.HDFSService;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

@Service
public class HDFSServiceImpl implements HDFSService {
    @Autowired
    HadoopTemplate hadoopTemplate;

    @Override
    public ListReturnInterface get_list(String path) {
        if (path != null) {
            try {
                boolean if_dir = hadoopTemplate.existDir(path, false);

                if (if_dir) {
                    if (!hadoopTemplate.existsFile(path)) throw new Exception("文件不存在");
                    List<EachFileDirInfo> tem_file_dir_list = new LinkedList<>();
                    for (FileStatus i : hadoopTemplate.list(path)) {
                        EachFileDirInfo tem = new EachFileDirInfo();
                        tem.setDir_or_file(i.isDirectory() ? "Dir" : "file");
                        String tem_path = i.getPath().toString();
                        tem.setPath(tem_path.substring(tem_path.indexOf("/", 7)));
                        tem_file_dir_list.add(tem);
                    }
                    DirContent dirContent = new DirContent();
                    EachFileDirInfo[] tem = new EachFileDirInfo[tem_file_dir_list.size()];
                    tem_file_dir_list.toArray(tem);
                    dirContent.setContent(tem);
                    dirContent.setStatus("dir");
                    return dirContent;
                } else {
                    FileContent fileContent = new FileContent();
                    fileContent.setContent(hadoopTemplate.read(true, path));
                    fileContent.setStatus("file");
                    return fileContent;
                }
            } catch (Exception e) {
                e.printStackTrace();
                FileContent fileContent = new FileContent();
                fileContent.setStatus("wrong");
                fileContent.setContent(e.getMessage());
                return fileContent;
            }
        } else {
            FileContent fileContent = new FileContent();
            fileContent.setStatus("wrong");
            fileContent.setContent("can't get parameter \"path\" or \"dir_or_file\"");
            return fileContent;
        }
    }

    @Override
    public String upload(String filepath, String content) {
        try {
            hadoopTemplate.write(filepath, content);
            return "文件上传成功";
        } catch (Exception e) {
            e.printStackTrace();
            return "文件保存失败 " + e.getMessage();
        }
    }

    @Override
    public String mk(String path) {
        try {
            hadoopTemplate.existDir(path, true);
            return "文件夹创建成功";
        } catch (Exception e) {
            e.printStackTrace();
            return "文件夹创建失败 " + e.getMessage();
        }
    }

    @Override
    public String rm(String delete_list) {
        try {
            for (String i : delete_list.split(",")) {
                hadoopTemplate.my_rm(i);
            }
            return "删除成功";
        } catch (Exception e) {
            e.printStackTrace();
            return "删除失败 " + e.getMessage();
        }
    }

    @Override
    public void download(HttpServletResponse response, String filepath) {
        String tem_file_save_path = "C:/tem/";

        try {
            String download_filename = filepath.substring(filepath.lastIndexOf('/'));
            File file = new File(tem_file_save_path + download_filename);
            // 考虑到存在下载到旧文件的问题，待以后更新根据最后修改时间决定是否重新下载
            // if (!file.exists()) hadoopTemplate.download(filepath, tem_file_save_path);
            hadoopTemplate.download(filepath, tem_file_save_path);

            response.setContentType("application/force-download");// 设置强制下载不打开
            //response.addHeader("Content-Disposition", "attachment;fileName=" + fileName);// 设置文件名
            //response.setContentType("multipart/form-data;charset=UTF-8");也可以明确的设置一下UTF-8，测试中不设置也可以。
            response.setHeader("Content-Disposition", "attachment;fileName=" + new String(download_filename.getBytes("GB2312"), "ISO-8859-1"));
            byte[] buffer = new byte[1024];
            FileInputStream fis = null;
            BufferedInputStream bis = null;
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis);
            OutputStream os = response.getOutputStream();
            int i = bis.read(buffer);
            while (i != -1) {
                os.write(buffer, 0, i);
                i = bis.read(buffer);
            }
        } catch (Exception e) {
            e.printStackTrace();
            response.setStatus(400);
        }
    }
}