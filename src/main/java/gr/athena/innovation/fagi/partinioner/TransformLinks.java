package gr.athena.innovation.fagi.partinioner;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author nkarag
 */
public class TransformLinks {
    
    public static void transform(String sourceLinksPath, String targetLinksPath){

        List newLinks = new ArrayList<>();

        System.out.println("Start partitioning..");
        Charset charset = Charset.forName("UTF-8");
        Path linksPath = Paths.get(sourceLinksPath);
        
        try {
            List<String> lines = Files.readAllLines(linksPath, charset);

            for (String line : lines) {
              String lineNew = line.replace("/name","");
              String lineNew2 = lineNew.replace("/brandname","");
              newLinks.add(lineNew2);
            }
        } catch (IOException e) {
            System.out.println(e);
        }        
        
        Path file = Paths.get(targetLinksPath);
        try {
            Files.write(file, newLinks, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            System.out.println(ex);
        }        
    }
}
