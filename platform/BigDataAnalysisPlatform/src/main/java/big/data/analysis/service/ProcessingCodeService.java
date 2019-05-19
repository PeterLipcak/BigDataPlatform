package big.data.analysis.service;

import big.data.analysis.entity.JavaProject;
import big.data.analysis.exception.CompilationException;
import big.data.analysis.exception.FlinkSubmissionException;
import big.data.analysis.property.FlinkProperties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

/**
 * Service responsible for managing code compilation and submission
 * @author Peter Lipcak, Masaryk University
 */
@Service
public class ProcessingCodeService {

    private String bdapPath = "/usr/local/bdap";
    private String flinkPath = "/usr/local/flink/bin/flink";
    private String mainClassPath = "/src/main/java/Main.java";
    private String dependenciesFile = "pom.xml";

    private String templateProjectPath;

    @Autowired
    public ProcessingCodeService(FlinkProperties flinkProperties) {
        this.templateProjectPath = flinkProperties.getTemplateProject();
    }

    /**
     * This method injects java code with dependencies to template project and store it in a local directory
     * @param javaProject project name, code and dependencies
     * @throws IOException when unable to save project
     */
    public void saveCode(JavaProject javaProject) throws IOException {
        File project = new File(bdapPath + "/projects/" + javaProject.getProjectName());
        FileUtils.deleteDirectory(project);

        if(!project.exists()){
            File sourceDirectory = new File(templateProjectPath);
            File targetDirectory= new File(bdapPath + "/projects/" + javaProject.getProjectName());
            FileUtils.copyDirectory(sourceDirectory, targetDirectory);
        }

        //Write Java code to Main.java file
        File mainJavaFile = new File(bdapPath + "/projects/" + javaProject.getProjectName() + mainClassPath);
        mainJavaFile.createNewFile();
        FileUtils.writeStringToFile(mainJavaFile, javaProject.getCode());

        //Inject dependencies to pom.xml file
        Path pomFile = new File(bdapPath + "/projects/" + javaProject.getProjectName() + "/" + dependenciesFile).toPath();
        Charset charset = StandardCharsets.UTF_8;
        String content = new String(Files.readAllBytes(pomFile), charset);
        content = content.replaceAll("PROJECT_NAME", javaProject.getProjectName());
        content = content.replaceAll("DEPENDENCIES_TO_BE_INJECTED", javaProject.getDependencies());
        Files.write(pomFile, content.getBytes(charset));

    }

    /**
     * This method is responsible for project compilation
     * @param javaProject project to be compiled
     * @param packageToJar if the code should also be packaged to jar (takes longer)
     * @return compilation result
     * @throws IOException
     * @throws InterruptedException
     * @throws CompilationException
     */
    public String compileCode(JavaProject javaProject, boolean packageToJar) throws IOException, InterruptedException, CompilationException {
        saveCode(javaProject);

        //Create process running mvn compile or mvn clean package in the template project directory
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

    /**
     * Compiles the code and if without errors then the code is also submitted to Flink
     * @param javaProject project to be compiled and submitted
     * @return submission result
     * @throws IOException
     * @throws InterruptedException
     * @throws CompilationException
     * @throws FlinkSubmissionException
     */
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
