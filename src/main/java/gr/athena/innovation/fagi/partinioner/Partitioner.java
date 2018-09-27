package gr.athena.innovation.fagi.partinioner;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Partitioning of RDF data that come from triplegeo and the slipo-eu ontology.
 * 
 * @author nkarag
 */
public class Partitioner {

    private static final Logger LOG = LogManager.getRootLogger();

    /**
     * 
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {

        String config = null;

        String arg;
        String value;

        int i = 0;

        while (i < args.length) {
            arg = args[i];
            if (arg.startsWith("-")) {
                if (arg.equals("-help")) {
                    LOG.info(PartitioningConstants.HELP);
                    System.exit(-1);
                }
            }
            value = args[i + 1];
            if (arg.equals("-config")) {
                config = value;
                break;
            }
            i++;
        }

        try {

            PartitionerInstance partitioner = new PartitionerInstance();
            partitioner.run(config);

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            LOG.info(PartitioningConstants.HELP);
            System.exit(-1);
        }
    }
}


