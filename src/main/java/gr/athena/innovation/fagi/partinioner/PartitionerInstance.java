package gr.athena.innovation.fagi.partinioner;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.io.BufferedReader;
import java.io.File;
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
import java.util.Collection;
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

        int partitionSize = Integer.parseInt(config.getLinkSize());
        String datasetA = config.getDatasetA();
        String datasetB = config.getDatasetB();
        String linksPath = config.getLinks();
        String outputDir = config.getOutputDir();

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

        long stop = System.currentTimeMillis();
        LOG.info(lines.size() + "links loaded in " + (stop - start) + "ms.");

        long start2 = System.currentTimeMillis();

        //the first goal is to partition the source datasets to n->m partitions that correspond to the links.
        List<List<String>> subLists = new ArrayList<>();

        int sublistIndex = 1;
        for (int i = 0; i < lines.size(); i += partitionSize) {

            List<String> sublist = lines.subList(i, Math.min(i + partitionSize, lines.size()));

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
        Multimap<String, String> linksMap = ArrayListMultimap.create();

        System.out.println("Number of partitions: " + subLists.size());

        sublistIndex = 1;
        for (List<String> subList : subLists) {
            Path partitionPath = getPartitionPath(outputDirPath, sublistIndex);
            LOG.info("Computing for partition path: " + partitionPath);
            //Integer group = subList.hashCode();
            for (String link : subList) {
                String[] parts = link.split(Constants.SPLIT);
                String idAPart = parts[0];
                String idBPart = parts[2];

                String idA = getResourceURI(idAPart);
                String idB = getResourceURI(idBPart);

                linksMap.put(idA, idB);

                mapForDatasetA.put(idA, partitionPath + Constants.Path.SLASH 
                        + Constants.Path.A + sublistIndex + Constants.Path.NT);
                mapForDatasetB.put(idB, partitionPath + Constants.Path.SLASH 
                        + Constants.Path.B + sublistIndex + Constants.Path.NT);

            }
            sublistIndex++;
        }

        //Here the two maps are ready to used for the actual partitioning.
        //Each map contains the URI for the corresponding dataset, along with the partition group as value.
        long stop2 = System.currentTimeMillis();

        long time2millis = stop2 - start2;
        String time2 = getFormattedTime(time2millis);

        LOG.info((int) (lines.size() / partitionSize) + " partitions created for links " + time2 + ".");

        long start3 = System.currentTimeMillis();

        //Begin partitioning
        //ExecutorService pool = Executors.newFixedThreadPool(2);
        //Callable<Long> callableA = new DatasetPartitioner(mapForDatasetA, datasetA);
        //Callable<Long> callableB = new DatasetPartitioner(mapForDatasetB, datasetB);
        //LOG.info("map a: " + mapForDatasetA);
        //LOG.info("map b: " + mapForDatasetB);
        DatasetPartitioner callableA = new DatasetPartitioner(mapForDatasetA, datasetA);

        LOG.info("Starting process...");

        //Future<Long> futureA = pool.submit(callableA);
        //Future<Long> futureB = pool.submit(callableB);
        //https://stackoverflow.com/questions/26226819/java-i-o-unexpected-performance-difference-between-sequentially-and-concurrentl?rq=1
        //Long threadTimeMillisA = futureA.get();
        Long threadTimeMillisA = callableA.call();
        String threadTimeA = getFormattedTime(threadTimeMillisA);
        LOG.info("Dataset A partitioned in " + threadTimeA + ".");

        DatasetPartitioner callableB = new DatasetPartitioner(mapForDatasetB, datasetB);
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

        //renameFiles(mapForDatasetA, mapForDatasetB);
    }

    private void renameFiles(Multimap<String, String> mapForDatasetA, Multimap<String, String> mapForDatasetB) 
            throws IOException {
        //rename with the standard names
        Collection<String> paths = new ArrayList<>();

        paths.addAll(mapForDatasetA.values());
        paths.addAll(mapForDatasetB.values());

        for(String val : paths){
            File oldfile = new File(val);
            int startIndex = val.lastIndexOf(Constants.Path.UNDERSCORE);
            int endIndex = val.lastIndexOf(Constants.Path.DOT);
            
            String name = val.substring(startIndex + 1, endIndex);
            int replaceIndex = val.lastIndexOf(Constants.Path.SLASH);
            String newPath = val.substring(0, replaceIndex);
            String newFilename = newPath + Constants.Path.SLASH + name + Constants.Path.NT;
            
            File newfile = new File(newFilename);

            if (oldfile.renameTo(newfile)) {
                LOG.info("Success, renamed: " + val + " to:\n " + newFilename);
            } else {
                throw new IOException(oldfile + " was not successfully renamed to " + newFilename);
            }
        }
    }

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

    public static String getFormattedTime(long millis) {
        String time = String.format("%02d min, %02d sec",
                TimeUnit.MILLISECONDS.toMinutes(millis),
                TimeUnit.MILLISECONDS.toSeconds(millis)
                - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
        );
        return time;
    }
}
