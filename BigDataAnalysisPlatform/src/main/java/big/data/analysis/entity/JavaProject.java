package big.data.analysis.entity;

import lombok.Data;

@Data
public class JavaProject {

    private String projectName;
    private String code;
    private String dependencies;

    public void setProjectName(String projectName) {
        this.projectName = projectName.replaceAll("\\s+","-");
    }
}
