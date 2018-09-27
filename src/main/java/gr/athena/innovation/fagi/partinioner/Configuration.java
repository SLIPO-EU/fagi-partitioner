package gr.athena.innovation.fagi.partinioner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Partitioning configuration.
 * 
 * @author nkarag
 */
public class Configuration {
    
    private static final Logger LOG = LogManager.getLogger(Configuration.class);
    
    private static Configuration configuration;

    private String datasetA;
    private String datasetB;
    private String links;
    private String linkSize;
    private EnumOutputMode fusionMode;
    private String outputDir;
    
    private Configuration() {
    }

    public static Configuration getInstance() {
        //lazy init
        if (configuration == null) {
            configuration = new Configuration();
        }

        return configuration;
    }

    public String getDatasetA() {
        return datasetA;
    }

    public void setDatasetA(String datasetA) {
        this.datasetA = datasetA;
    }

    public String getDatasetB() {
        return datasetB;
    }

    public void setDatasetB(String datasetB) {
        this.datasetB = datasetB;
    }

    public String getLinks() {
        return links;
    }

    public void setLinks(String links) {
        this.links = links;
    }

    public String getLinkSize() {
        return linkSize;
    }

    public void setLinkSize(String linkSize) {
        this.linkSize = linkSize;
    }

    public EnumOutputMode getFusionMode() {
        return fusionMode;
    }

    public void setFusionMode(EnumOutputMode fusionMode) {
        this.fusionMode = fusionMode;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }
}
