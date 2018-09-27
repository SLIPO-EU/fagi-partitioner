package gr.athena.innovation.fagi.partinioner;

import java.io.File;
import java.io.IOException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author nkarag
 */
public class ConfigParser {

    private static final org.apache.logging.log4j.Logger LOG = LogManager.getLogger(ConfigParser.class);

    /**
     * Parses the configuration XML and produces the configuration object.
     * 
     * @param configurationPath the configuration file path.
     * @return the configuration object.
     * @throws WrongInputException indicates that something is wrong with the input.
     */
    public Configuration parse(String configurationPath) throws WrongInputException {

        LOG.info("Parsing configuration: " + configurationPath);
        Configuration configuration = Configuration.getInstance();

        try {

            File fXmlFile = new File(configurationPath);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);

            doc.getDocumentElement().normalize();

            NodeList out = doc.getElementsByTagName(PartitioningConstants.OUTPUT_DIR);
            String outputDir = out.item(0).getTextContent();
            
            if(!new File(outputDir).isDirectory()){
                throw new WrongInputException("Output path is not a directory. Specify an existing directory path.");
            }
            configuration.setOutputDir(outputDir);

            NodeList a = doc.getElementsByTagName(PartitioningConstants.DATASET_A);
            String datasetA = a.item(0).getTextContent();
            configuration.setDatasetA(datasetA);

            NodeList b = doc.getElementsByTagName(PartitioningConstants.DATASET_B);
            String datasetB = b.item(0).getTextContent();
            configuration.setDatasetB(datasetB);

            NodeList l = doc.getElementsByTagName(PartitioningConstants.LINKS);
            String links = l.item(0).getTextContent();
            configuration.setLinks(links);
            
            NodeList s = doc.getElementsByTagName(PartitioningConstants.LINK_SIZE);
            String size = s.item(0).getTextContent();
            configuration.setLinkSize(size);

            NodeList m = doc.getElementsByTagName(PartitioningConstants.FUSION_MODE);
            String modeString = m.item(0).getTextContent();
            EnumOutputMode mode = EnumOutputMode.fromString(modeString.toUpperCase());
            switch(mode) {
                case AA_MODE:
                case AB_MODE:
                case A_MODE:
                case BB_MODE:
                case BA_MODE:
                case B_MODE:
                case L_MODE:
                    configuration.setFusionMode(mode);
                    break;
                default:
                    LOG.info("Mode not supported.");
                    throw new UnsupportedOperationException("Wrong Output mode!");               
            }
        } catch (ParserConfigurationException | SAXException | IOException | DOMException e) {
            LOG.fatal("Exception occured while parsing the configuration: "
                    + configurationPath + "\n" + e);
            throw new WrongInputException(e.getMessage());
        }

        return configuration;
    }
}
