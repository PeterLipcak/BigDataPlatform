package big.data.analysis.service;

import big.data.analysis.entity.Dataset;
import big.data.analysis.exception.DatasetStorageException;
import big.data.analysis.property.DatasetStorageProperties;
import big.data.analysis.utils.EnvironmentVariablesHelper;
import big.data.analysis.utils.HdfsHelper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DatasetsStorageService {

    private static final Logger logger = LoggerFactory.getLogger(DatasetsStorageService.class);

    private final Path localStorageDatasetLocation;
    private final String hdfsStorageDatasetLocation;

    @Autowired
    public DatasetsStorageService(DatasetStorageProperties datasetStorageProperties) {
        this.localStorageDatasetLocation = Paths.get(datasetStorageProperties.getUploadLocalDir())
                .toAbsolutePath().normalize();

        this.hdfsStorageDatasetLocation = datasetStorageProperties.getUploadHdfsDir();

        try {
            Files.createDirectories(this.localStorageDatasetLocation);
        } catch (Exception ex) {
            throw new DatasetStorageException("Could not create the directory where the uploaded files will be stored.", ex);
        }
    }


    public String storeDatasetLocally(MultipartFile file) {
        // Normalize file name
        String fileName = StringUtils.cleanPath(file.getOriginalFilename());

        try {
            // Check if the file's name contains invalid characters
            if(fileName.contains("..")) {
                throw new DatasetStorageException("Sorry! Filename contains invalid path sequence " + fileName);
            }

            // Copy file to the target location (Replacing existing file with the same name)
            Path targetLocation = this.localStorageDatasetLocation.resolve(fileName);
            Files.copy(file.getInputStream(), targetLocation, StandardCopyOption.REPLACE_EXISTING);

            return fileName;
        } catch (IOException ex) {
            throw new DatasetStorageException("Could not store file " + fileName + ". Please try again!", ex);
        }
    }

    public void uploadDatasetToHdfs(String datasetName) throws DatasetStorageException{
        HdfsHelper.uploadFileToHdfs(localStorageDatasetLocation.resolve(datasetName).toString(), hdfsStorageDatasetLocation + datasetName);
    }

    public List<Dataset> getDatasetPreviews(){
        Configuration hdfsConf = new Configuration();
        hdfsConf.addResource(new org.apache.hadoop.fs.Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        hdfsConf.addResource(new org.apache.hadoop.fs.Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        hdfsConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hdfsConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(EnvironmentVariablesHelper.getHdfsIpPort() + hdfsStorageDatasetLocation), hdfsConf);
        } catch (Exception e) {
            throw new DatasetStorageException("Could not receive datasets", e);
        }
        List<Dataset> datasets = new ArrayList<>();

        try {
            FileStatus[] fileStatus = fs.listStatus(new org.apache.hadoop.fs.Path(EnvironmentVariablesHelper.getHdfsIpPort() + hdfsStorageDatasetLocation));

            for (FileStatus status : fileStatus) {
                InputStream is = fs.open(status.getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                String line;

                List<String> preview = new ArrayList<>();

                for(int i = 0; i < 5; i++){
                    line = br.readLine();
                    if(line == null)break;
                    if (line.isEmpty()) {
                        continue;
                    }
                    preview.add(line);
                }

                Dataset dataset = Dataset
                        .builder()
                        .datasetName(status.getPath().getName())
                        .size(status.getLen())
                        .preview(preview)
                        .build();

                datasets.add(dataset);
            }
            fs.close();
        }catch (IOException e){
            throw new DatasetStorageException("Could not receive datasets", e);
        }

        return datasets;
    }

    public void deleteDatasetFromHdfs(String datasetName)throws DatasetStorageException{
        HdfsHelper.deleteFileFromHdfs(hdfsStorageDatasetLocation + datasetName);
    }



}
