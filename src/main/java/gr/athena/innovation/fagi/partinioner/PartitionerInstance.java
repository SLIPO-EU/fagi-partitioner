package gr.athena.innovation.fagi.partinioner;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import static java.nio.file.Files.newInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;

/**
 *
 * @author nkarag
 */
public class PartitionerInstance {

    private static final org.apache.logging.log4j.Logger LOG = LogManager.getLogger(PartitionerInstance.class);

    public void run(String configPath) throws IOException, WrongInputException {

        ConfigParser parser = new ConfigParser();
        Configuration config = parser.parse(configPath);

        String datasetAPath = config.getDatasetA();
        String datasetBPath = config.getDatasetB();
        String unlinkedAPath = config.getUnlinkedPathA();
        String unlinkedBPath = config.getUnlinkedPathB();
        int partitions = config.getPartitions();
        String linksPath = config.getLinks();
        String outputDir = config.getOutputDir();

        Path path = Paths.get(linksPath);
        int lineCount = (int) Files.lines(path).count();

        // Ensure no more than the requested number of partitions is created  
        int linksInEachPartition = (lineCount + partitions - 1) / partitions;

        LOG.info("Process started.");

        long start = System.currentTimeMillis();

        Path outputDirPath = Files.createDirectories(Paths.get(outputDir));

        //The following List read all lines of links in memory. Can take the whole links file,
        //or a part of a bigger links file as input. Here we use the whole file that contains all links.
        //List<String> lines = Files.readAllLines(linksPath, Charset.forName("UTF-8"));
        List<String> lines;
        int count = 0;
        try (BufferedReader reader = newBufferedReader(Paths.get(linksPath), Charset.forName("UTF-8"))) {
            lines = new ArrayList<>();
            for (;;) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }

                lines.add(line);
                count++;
                if (count % 1000000 == 0) {
                    LOG.info("Links added: " + count);
                }
            }
        }
        if (lines.size() != lineCount)
            throw new IllegalStateException("The number of links is different from previous count!");

        long stop = System.currentTimeMillis();
        LOG.info(lines.size() + " links loaded in " + (stop - start) + "ms.");

        long start2 = System.currentTimeMillis();

        //the first goal is to partition the source datasets to n->m partitions that correspond to the links.
        List<List<String>> subLists = new ArrayList<>();

        // Generate directory structure for (exactly) the number of partitions requested 
        int sublistIndex = 1;
        for (int i = 0, n = 0; n < partitions; i += linksInEachPartition, n++) {

            List<String> sublist = i < lineCount? 
                lines.subList(i, Math.min(i + linksInEachPartition, lineCount)) : Collections.emptyList();

            Path partitionPath = getPartitionPath(outputDirPath, sublistIndex);
            Files.createDirectories(partitionPath);

            Path sublinksPath = Paths.get(partitionPath + Constants.Path.SLASH + Constants.Path.LINKS
                    + Constants.Path.UNDERSCORE + sublistIndex + Constants.Path.NT);
            Files.write(sublinksPath, sublist, StandardCharsets.UTF_8);

            subLists.add(sublist);
            sublistIndex++;
        }

        //todo: user Properties/mapdb for 'on-disk' map
        Multimap<String, String> mapForDatasetA = ArrayListMultimap.create();
        Multimap<String, String> mapForDatasetB = ArrayListMultimap.create();

        //Use the linkedA, linkedB hashsets to write the unlinked entities in files 
        //when reading the source datasets at DatasetPartitioner.
        HashSet<String> linkedA = new HashSet<>();
        HashSet<String> linkedB = new HashSet<>();

        System.out.println("Number of partitions: " + subLists.size());

        sublistIndex = 1;
        for (List<String> subList : subLists) {
            Path partitionPath = getPartitionPath(outputDirPath, sublistIndex);
            LOG.info("Computing for partition path: " + partitionPath);
            //Integer group = subList.hashCode();
            if(subList.isEmpty()){
                mapForDatasetA.put("", partitionPath + Constants.Path.SLASH
                        + Constants.Path.A + sublistIndex + Constants.Path.NT);
                mapForDatasetB.put("", partitionPath + Constants.Path.SLASH
                        + Constants.Path.B + sublistIndex + Constants.Path.NT);
            }

            int lCount = 0;
            for (String link : subList) {

                processLinkNT(link, linkedA, linkedB, mapForDatasetA, partitionPath, sublistIndex, mapForDatasetB);

                lCount++;
                if (lCount % 1000000 == 0) {
                    LOG.info("sublistIndex: " + sublistIndex + " - links processed: " + lCount);
                    //break;
                }

            }
            sublistIndex++;
        }

        //Here the two maps are ready to used for the actual partitioning.
        //Each map contains the URI for the corresponding dataset, along with the partition group as value.
        long stop2 = System.currentTimeMillis();

        long time2millis = stop2 - start2;
        String time2 = getFormattedTime(time2millis);

        LOG.info((int) (lines.size() / linksInEachPartition) + " non-empty partitions created for " + lines.size() 
                + " links. Time: " + time2 + ".");

        LOG.info("Total partitions created: " + subLists.size());
        
        long start3 = System.currentTimeMillis();

        //Begin partitioning
        //ExecutorService pool = Executors.newFixedThreadPool(2);
        //Callable<Long> callableA = new DatasetPartitioner(mapForDatasetA, datasetAPath);
        //Callable<Long> callableB = new DatasetPartitioner(mapForDatasetB, datasetBPath);
        //LOG.info("map a: " + mapForDatasetA);
        //LOG.info("map b: " + mapForDatasetB);
        DatasetPartitioner callableA = new DatasetPartitioner(mapForDatasetA, linkedA, datasetAPath, unlinkedAPath);

        LOG.info("Starting process...");

        //Future<Long> futureA = pool.submit(callableA);
        //Future<Long> futureB = pool.submit(callableB);
        //https://stackoverflow.com/questions/26226819/java-i-o-unexpected-performance-difference-between-sequentially-and-concurrentl?rq=1
        //Long threadTimeMillisA = futureA.get();
        Long threadTimeMillisA = callableA.call();
        String threadTimeA = getFormattedTime(threadTimeMillisA);
        LOG.info("Dataset A partitioned in " + threadTimeA + ".");

        DatasetPartitioner callableB = new DatasetPartitioner(mapForDatasetB, linkedB, datasetBPath, unlinkedBPath);
        Long threadTimeMillisB = callableB.call();
        //Long threadTimeMillisB = futureB.get();
        String threadTimeB = getFormattedTime(threadTimeMillisB);
        LOG.info("Dataset B partitioned in " + threadTimeB + ".");

        //LOG.info("Shutting down pool...");
        //pool.shutdown();
        long stop3 = System.currentTimeMillis();
        long totalMillis = stop3 - start3;

        String totalTime = getFormattedTime(totalMillis);
        LOG.info("Total time: " + totalTime);

    }

    private void processLinkNT(String link, HashSet<String> linkedA, HashSet<String> linkedB, Multimap<String, String> mapForDatasetA, Path partitionPath, int sublistIndex, Multimap<String, String> mapForDatasetB) {
        String[] parts = link.split(Constants.SPLIT);
        String idAPart = parts[0];
        String idBPart = parts[2];
        
        String idA = getResourceURI(idAPart);
        String idB = getResourceURI(idBPart);
        
        linkedA.add(idA);
        linkedB.add(idB);
        
        mapForDatasetA.put(idA, partitionPath + Constants.Path.SLASH
                + Constants.Path.A + sublistIndex + Constants.Path.NT);
        mapForDatasetB.put(idB, partitionPath + Constants.Path.SLASH
                + Constants.Path.B + sublistIndex + Constants.Path.NT);
    }

//    private void processLinkCSV(String link, HashSet<String> linkedA, HashSet<String> linkedB, Multimap<String, String> mapForDatasetA, Path partitionPath, int sublistIndex, Multimap<String, String> mapForDatasetB) {
//        String[] parts = link.split(Constants.SPLIT);
//        String idAPart = parts[0];
//        String idBPart = parts[1];
//        
//                
//        String idA = getResourceURI(idAPart);
//        String idB = getResourceURI(idBPart);
//        
//        linkedA.add(idA);
//        linkedB.add(idB);
//        
//        mapForDatasetA.put(idA, partitionPath + Constants.Path.SLASH
//                + Constants.Path.A + sublistIndex + Constants.Path.NT);
//        mapForDatasetB.put(idB, partitionPath + Constants.Path.SLASH
//                + Constants.Path.B + sublistIndex + Constants.Path.NT);
//    }

    private Path getPartitionPath(Path resultPath, int sublistIndex) {

        Path partitionPath = Paths.get(resultPath + Constants.Path.SLASH + Constants.Path.PARTITION
                + Constants.Path.UNDERSCORE + sublistIndex);

        return partitionPath;
    }

    public static BufferedReader newBufferedReader(Path path, Charset cs) throws IOException {
        CharsetDecoder decoder = cs.newDecoder();
        Reader reader = new InputStreamReader(newInputStream(path), decoder);
        return new BufferedReader(reader);
    }

    public static String getResourceURI(String part) {
        int endPosition = StringUtils.lastIndexOf(part, Constants.Path.SLASH);
        int startPosition = StringUtils.ordinalIndexOf(part, Constants.Path.SLASH, 5) + 1;

        String result;
        if (part.substring(startPosition).contains(Constants.Path.SLASH)) {
            result = part.subSequence(startPosition, endPosition).toString();
        } else {
            result = part.subSequence(startPosition, part.length() - 1).toString();
        }

        return result;
    }

    //faster but not generic
//    public static String getResourceURI(String part) {
//        int endPosition = StringUtils.lastIndexOf(part, Constants.Path.SLASH);
//
//        String result = part.subSequence(24, 60).toString();
//        return result;
//    }

    public static String getFormattedTime(long millis) {
        String time;
        if(millis < 1000){
            time = millis + "ms";
        } else {
            time = String.format("%02d min, %02d sec",
                    TimeUnit.MILLISECONDS.toMinutes(millis),
                    TimeUnit.MILLISECONDS.toSeconds(millis)
                    - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
            );
        }
        return time;
    }
}
