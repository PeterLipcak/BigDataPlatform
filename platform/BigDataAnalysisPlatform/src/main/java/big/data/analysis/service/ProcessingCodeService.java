package big.data.analysis.service;

import big.data.analysis.entity.JavaProject;
import big.data.analysis.exception.CompilationException;
import big.data.analysis.exception.FlinkSubmissionException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Service
public class ProcessingCodeService {

    private static final Logger logger = LoggerFactory.getLogger(IngestionService.class);

    private String bdapPath = "/usr/local/bdap";
    private String flinkPath = "/usr/local/flink/bin/flink";
    private String mainClassPath = "/src/main/java/Main.java";
    private String dependenciesFile = "pom.xml";
    private String defaultProject = "BDAP-template";

    public void saveCode(JavaProject javaProject) throws IOException {
        File project = new File(bdapPath + "/projects/" + javaProject.getProjectName());
        FileUtils.deleteDirectory(project);

        if(!project.exists()){
            File sourceDirectory = new File(bdapPath + "/" + defaultProject);
            File targetDirectory= new File(bdapPath + "/projects/" + javaProject.getProjectName());
            FileUtils.copyDirectory(sourceDirectory, targetDirectory);
        }

        File mainJavaFile = new File(bdapPath + "/projects/" + javaProject.getProjectName() + mainClassPath);
        mainJavaFile.createNewFile();
        FileUtils.writeStringToFile(mainJavaFile, javaProject.getCode());

        Path pomFile = new File(bdapPath + "/projects/" + javaProject.getProjectName() + "/" + dependenciesFile).toPath();
        Charset charset = StandardCharsets.UTF_8;
        String content = new String(Files.readAllBytes(pomFile), charset);
        content = content.replaceAll("PROJECT_NAME", javaProject.getProjectName());
        content = content.replaceAll("DEPENDENCIES_TO_BE_INJECTED", javaProject.getDependencies());
        Files.write(pomFile, content.getBytes(charset));

    }

    public String compileCode(JavaProject javaProject, boolean packageToJar) throws IOException, InterruptedException, CompilationException {
        saveCode(javaProject);

        ProcessBuilder builder = new ProcessBuilder();
        builder.directory(new File(bdapPath + "/projects/" + javaProject.getProjectName()));
        if(packageToJar){
            builder.command("mvn","clean","package");
        }else{
            builder.command("mvn","compile");
        }

        builder.redirectErrorStream(true);

        Process process = builder.start();
        InputStream is = process.getInputStream();

        String compilationOutput = IOUtils.toString(is, Charset.defaultCharset());

        if(process.waitFor() != 0)throw new CompilationException(compilationOutput);

        return compilationOutput;
    }

    public String submitCode(JavaProject javaProject) throws IOException, InterruptedException, CompilationException, FlinkSubmissionException {
        compileCode(javaProject,true);

        ProcessBuilder builder = new ProcessBuilder();
        builder.directory(new File(bdapPath + "/projects/" + javaProject.getProjectName() + "/target"));
        builder.command(flinkPath, "run", "-d", javaProject.getProjectName() + "-1.0-SNAPSHOT-jar-with-dependencies.jar");
        builder.redirectErrorStream(true);

        Process process = builder.start();
        InputStream is = process.getInputStream();

        String submissionOutput = IOUtils.toString(is, Charset.defaultCharset());

        if(process.waitFor() != 0)throw new FlinkSubmissionException(submissionOutput);

        return submissionOutput;
    }

}
