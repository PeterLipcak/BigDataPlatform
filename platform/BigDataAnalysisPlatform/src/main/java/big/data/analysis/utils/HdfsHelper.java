package big.data.analysis.utils;

import big.data.analysis.exception.DatasetStorageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Helper for working with HDFS
 * @author Peter Lipcak, Masaryk University
 */
public class HdfsHelper {

    /**
     * Get hdfs configuration
     * @return configuration object
     */
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


    /**
     * Uplaods the file from local file system to hdfs
     * @param localPath path to local file
     * @param hdfsPath path to hdfs destination
     * @throws DatasetStorageException when problem during upload
     */
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
    }

    /**
     * Deletes dataset from hdfs
     * @param hdfsPath path to dataset
     * @throws DatasetStorageException when unable to delete
     */
    public static void deleteFileFromHdfs(String hdfsPath) throws DatasetStorageException{
        try {
            FileSystem fs = FileSystem.get(getHadoopConf());
            fs.delete(new Path(hdfsPath),false);
        } catch (IOException e) {
            throw new DatasetStorageException("Could delete file from hdfs.", e);
        }
    }

}
