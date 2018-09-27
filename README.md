# Fagi-partitioner
This project is used for partitioning RDF data that follow the slipo-eu ontology.
___

### Building from source
Clone the project to a preferred location:

`git clone https://github.com/SLIPO-EU/fagi-partitioner.git fagi-partitioner`

Then, go the root directory of the project (fagi-partitioner) and run:
`mvn clean install`

### Run Fagi-gis from command line
Go to the target directory of the project and run:

`java -jar fagi-partitioner-SNAPSHOT.jar -config /path/to/config.xml`

### Config.xml file

Inside the resources directory of the project there is a config.xml file. 

`datasetA` the path of the first dataset.
`datasetB` the path of the second dataset.
`links` the path of the links file.
`linkSize` the number of links to use for each partition group.
`outputDir` the output directory path for the partitioning results.
