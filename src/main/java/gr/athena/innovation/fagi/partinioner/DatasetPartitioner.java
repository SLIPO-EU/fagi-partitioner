package gr.athena.innovation.fagi.partinioner;

import com.google.common.collect.Multimap;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
    private final HashSet<String> linked;
    private final String datasetPath;
    private final String unlinkedPath;
    private final Path file;

    public DatasetPartitioner(Multimap<String, String> map, HashSet<String> linked, String datasetPath, 
            String unlinkedPath) throws IOException{
        
        this.map = map;
        this.linked = linked;
        this.datasetPath = datasetPath;
        this.unlinkedPath = unlinkedPath;

        File f = new File(unlinkedPath);
        if (f.exists()) {
            f.delete();
            f.createNewFile();
            LOG.info("exists. clearing.");
        } else {
            f.createNewFile();
            LOG.info("does not exist. created.");
        }
        file = Paths.get(unlinkedPath);
    }

    public Long call() throws IOException{
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
            List<String> bufferedLines = new ArrayList<>();
            for (String line; (line = br.readLine()) != null;) {
                String[] parts = line.split(" ");
                String idPart = parts[0];
                String id = PartitionerInstance.getResourceURI(idPart);

                if(!linked.contains(id)){
                    bufferedLines.add(line);
                    if(bufferedLines.size() > 1000){
                        writeUnlinked(bufferedLines);
                        bufferedLines.clear();
                    }
                }

                Collection<String> groups = map.get(id);

                for(String group : groups){
                    BufferedWriter bufferedWriter = writerMapA.get(group);
                    bufferedWriter.append(line);
                    bufferedWriter.newLine();
                }
            }

            if(!bufferedLines.isEmpty()){
                writeUnlinked(bufferedLines);
                bufferedLines.clear();
            }
            LOG.info("Closing reader..");
        } catch (IOException ex) {
            LOG.error(ex);
            throw new IOException(ex);
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
        
        LOG.info("Flushing time " + (stop3 - start3));
        
        long stop = System.currentTimeMillis();
        LOG.info("thread returning. Time running was: " + (stop-start));
        return stop-start;
    }

    private void writeUnlinked(List<String> lines) throws FileNotFoundException, IOException {
        Files.write(file, lines, Charset.forName("UTF-8"), StandardOpenOption.APPEND);
    }
}
