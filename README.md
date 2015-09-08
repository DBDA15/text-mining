# text-mining

This project is an implementation of the [Snowball algorithm](http://www.cs.columbia.edu/~gravano/Papers/2000/dl00.pdf) on the distributed data processing platforms [Apache Spark](https://spark.apache.org/) and [Apache Flink](https://flink.apache.org/).

## Spark

In order to use the Spark version of the implementation cd into MainTask-spark/MainTask and compile the project using `mvn package`.

Then you can use the following command to start the program:

```
$PATH_TO_SPARK_SUBMIT --total-executor-cores $EXECUTOR_CORES --class de.hpi.fgis.dbda.textmining.maintask.App --master $SPARK_MASTER target/maintask-0.0.1-SNAPSHOT.jar $OUTPUT_PATH $PATH_TO_SEED_TUPLES $INPUT_FILES
```

where the variables mean the following:

$PATH_TO_SPARK_SUBMIT: the path to your spark submit executable.

$EXECUTOR_CORES: the number of workers your program is run on.

$SPARK_MASTER: the address of the spark master node. 

$OUTPUT_PATH: the path to write the ouput (Organization, Location tuples) to.

$PATH_TO_SEED_TUPLES: the path to the seed tuples the algorithm needs. These should be a tsv file with the format ORG \t LOC

$INPUT_FILES: a arbitrary number of input files. In our tests the New York Times archive was used. The files should already be NER tagged and split into one sentence per line.

## Flink

In order to use the Flink version of the implementation cd into MainTask-flink/MainTask-flink and compile the project using `mvn package`.

Then you can use the following command to start the program:

```
$PATH_TO_FLINK run --parallelism $PARALLELISM --class de.hpi.fgis.dbda.textmining.MainTask_flink.App target/MainTask-flink-0.0.1-SNAPSHOT-flink-fat-jar.jar --windowSize $WINDOWSIZE --minimalClusterSize $MINIMAL_CLUSTER_SIZE --degreeOfMatchThreshold $DEGREE_OF_MATCH_THRESHOLD --similarityThreshold $SIMILARITY_THRESHOLD --tupleConfidenceThreshold $TUPLE_CONFIDENCE_THRESHOLD --maxDistance $MAX_DISTANCE --iterations $ITERATIONS $ALREADY_TAGGED --output $OUTPUT_PATH --seedTuples $PATH_TO_SEED_TUPLES $INPUT_FILES
```

where the variables mean the following:

$PATH_TO_FLINK: the path to your flink executable.

$PARALLELISM: the number of workers your program is run on.

$WINDOWSIZE: the windows of words that belong to a tuple context.

$MINIMAL_CLUSTER_SIZE: the minimal amount of patterns a cluster should consist of to be used further.

$DEGREE_OF_MATCH_THRESHOLD: the similarity threshold for matching patterns with tuple contexts.

$SIMILARITY_THRESHOLD: the similarity threshold for clustering the patterns.

$TUPLE_CONFIDENCE_THRESHOLD: the confidence threshold for filtering out candidate tuples.

$MAX_DISTANCE: the maximum distance between found Organizations and Locations.

$ITERATIONS: the number of iterations the algorithm is run.

$ALREADY_TAGGED: use the --alreadyTagged flag if the input files are already split into sentences and NER tagged.

$OUTPUT_PATH: the path to write the ouput (Organization, Location tuples) to.

$PATH_TO_SEED_TUPLES: the path to the seed tuples the algorithm needs. These should be a tsv file with the format ORG \t LOC

$INPUT_FILES: a arbitrary number of input files. In our tests the New York Times archive was used. The files should already be NER tagged and split into one sentence per line.