package big.data.analysis.utils;

import big.data.analysis.exception.DatasetStorageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsHelper {

    private static Configuration getHadoopConf(){
        Configuration hdfsConf = new Configuration();
        hdfsConf.addResource(new org.apache.hadoop.fs.Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        hdfsConf.addResource(new org.apache.hadoop.fs.Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        hdfsConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hdfsConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        hdfsConf.set("fs.hdfs.impl.disable.cache", "false");
        hdfsConf.set("fs.defaultFS", EnvironmentVariablesHelper.getHdfsIpPort());
        return hdfsConf;
    }

    public static void uploadFileToHdfs(String localPath, String hdfsPath) throws DatasetStorageException{

        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("hadoop", "fs", "-put", localPath, hdfsPath);
        try {
            Process process = processBuilder.start();
            process.waitFor();
        } catch (IOException e) {
            throw new DatasetStorageException("Could not copy file to hdfs.", e);
        } catch (InterruptedException e) {
            throw new DatasetStorageException("Could not copy file to hdfs.", e);
        }
//        try {
//            FileSystem fs = FileSystem.get(getHadoopConf());
//            fs.copyFromLocalFile(true, new Path(localPath), new Path(hdfsPath));
//        } catch (IOException e) {
//            throw new DatasetStorageException("Could not copy file to hdfs.", e);
//        }
    }

    public static void deleteFileFromHdfs(String hdfsPath) throws DatasetStorageException{
        try {
            FileSystem fs = FileSystem.get(getHadoopConf());
            fs.delete(new Path(hdfsPath),false);
        } catch (IOException e) {
            throw new DatasetStorageException("Could delete file from hdfs.", e);
        }
    }

}
