package gr.athena.innovation.fagi.partinioner;

/**
 * Constants used for XML tags.
 * 
 * @author nkarag
 */
public class PartitioningConstants {

    /**
     * Filename for the configuration XML file.
     */
    public static final String CONFIG_XML = "config.xml";

    /**
     * Filename for the configuration XSD file that describes the configuration XML file.
     */
    public static final String CONFIG_XSD = "config.xsd";

    /**
     * Name for left dataset tag in XML.
     */
    public static final String PARTITIONING = "partitioning";

    /**
     * Name for left dataset tag in XML.
     */
    public static final String DATASET_A = "datasetA";

    /**
     * Name for right dataset tag in XML.
     */
    public static final String DATASET_B = "datasetB";

    /**
     * Name for links tag in XML.
     */
    public static final String LINKS = "links";

    /**
     * Name for linkSize tag in XML.
     */
    public static final String LINK_SIZE = "linkSize";

    /**
     * Name for fusion-mode tag in XML.
     */
    public static final String FUSION_MODE = "fusionMode";

    /**
     * Name for output directory tag in XML.
     */
    public static final String OUTPUT_DIR = "outputDir";
    
    /**
     * Help message.
     */
    public static final String HELP = "Usage:\n java -jar partitioner.jar -config <configPath>";

}