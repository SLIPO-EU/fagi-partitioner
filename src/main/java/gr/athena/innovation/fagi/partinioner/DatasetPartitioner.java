package gr.athena.innovation.fagi.partinioner;

import com.google.common.collect.Multimap;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author nkarag
 */
public class DatasetPartitioner {

    private static final Logger LOG = LogManager.getLogger(DatasetPartitioner.class);
    private final Multimap<String, String> map;
    private final String datasetPath;

    public DatasetPartitioner(Multimap<String, String> map, String datasetPath){
        this.map = map;
        this.datasetPath = datasetPath;
    }

    public Long call(){
        LOG.info("Thread dataset: " + datasetPath);
        long start = System.currentTimeMillis();
        
        Map<String, BufferedWriter> writerMapA = new HashMap<>();
        Set<String> filepathsSetA = new HashSet<>();
        filepathsSetA.addAll(map.values());
        
        for(String filepath : filepathsSetA){
            BufferedWriter bufferedWriter;
            try {
                bufferedWriter = new BufferedWriter(new FileWriter(filepath, true));
                writerMapA.put(filepath, bufferedWriter);
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }

        LOG.info("Buffered writers mapping ready.");

        try (BufferedReader br = Files.newBufferedReader(Paths.get(datasetPath), StandardCharsets.UTF_8)) {
            LOG.info("Opening reader..");
            for (String line; (line = br.readLine()) != null;) {
                String[] parts = line.split(" ");
                String idPart = parts[0];
                String id = PartitionerInstance.getResourceURI(idPart);

                Collection<String> groups = map.get(id);

                for(String group : groups){
                    BufferedWriter bufferedWriter = writerMapA.get(group);
                    bufferedWriter.append(line);
                    bufferedWriter.newLine();
                }
            }
            LOG.info("Closing reader..");
        } catch (IOException ex) {
            LOG.error(ex);
        }

        LOG.info("Flushing data...");
        
        long start3 = System.currentTimeMillis();
        for(BufferedWriter writer : writerMapA.values()){
            try {
                writer.flush();
                writer.close();
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        long stop3 = System.currentTimeMillis();
        
        LOG.info("(time3) flushing time " + (stop3 - start3));
        
        long stop = System.currentTimeMillis();
        LOG.info("thread returning. Time running was: " + (stop-start));
        return stop-start;
    }

}
