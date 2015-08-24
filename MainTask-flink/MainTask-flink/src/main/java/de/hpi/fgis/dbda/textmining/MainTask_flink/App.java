package de.hpi.fgis.dbda.textmining.MainTask_flink;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import de.hpi.fgis.dbda.textmining.functions.CalculateBestPatternSimilarity;
import de.hpi.fgis.dbda.textmining.functions.CalculatePatternConfidences;
import de.hpi.fgis.dbda.textmining.functions.CandidateTupleConfidenceCalculator;
import de.hpi.fgis.dbda.textmining.functions.CandidateTupleConfidenceFilter;
import de.hpi.fgis.dbda.textmining.functions.CandidateTupleSimplifier;
import de.hpi.fgis.dbda.textmining.functions.CandidateTuplesMapper;
import de.hpi.fgis.dbda.textmining.functions.ClusterCentroids;
import de.hpi.fgis.dbda.textmining.functions.ClusterCentroidsMapper;
import de.hpi.fgis.dbda.textmining.functions.ClusterPartition;
import de.hpi.fgis.dbda.textmining.functions.ExtractOrganizationSentenceTuples;
import de.hpi.fgis.dbda.textmining.functions.FilterByTags;
import de.hpi.fgis.dbda.textmining.functions.MapPositivesAndNegatives;
import de.hpi.fgis.dbda.textmining.functions.MapSeedTuplesFromStrings;
import de.hpi.fgis.dbda.textmining.functions.RawPatternsMapper;
import de.hpi.fgis.dbda.textmining.functions.ReducePositivesAndNegatives;
import de.hpi.fgis.dbda.textmining.functions.SearchForTagOccurences;
import de.hpi.fgis.dbda.textmining.functions.SearchRawPatterns;
import de.hpi.fgis.dbda.textmining.functions.SeedTuplesExtractor;
import de.hpi.fgis.dbda.textmining.functions.TextSegmentMapper;
import de.hpi.fgis.dbda.textmining.functions.TupleGenerationPatternsFinder;
import de.hpi.fgis.dbda.textmining.functions.UniqueOrganizationReducer;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class App {


	/** Stores execution parameters of this job. */
	private final Parameters parameters;

	/** Maps attribute indexes to files. */
	private Int2ObjectMap<String> filesByAttributeIndexOffset;

    //Initialize entity tags for the relation extraction
    final List<String> task_entityTags = new ArrayList<>();

	public static void main(String[] args) throws Exception {
		App snowball = new App(args);
		snowball.run();
	}

	public App(String[] args) {
		this.parameters = Parameters.parse(args);
	}

	private void run() throws Exception {
		// Load the execution environment.
		final ExecutionEnvironment env = createExecutionEnvironment();

        task_entityTags.add("ORGANIZATION");
        task_entityTags.add("LOCATION");

		// Read and parse the input sentences.
		this.filesByAttributeIndexOffset = new Int2ObjectOpenHashMap<>();
		
		DataSet<String> sentencesWithTags = null;
		
		if (parameters.step == 0 || parameters.step == 1) {
			Collection<String> inputPaths = loadInputPaths();
			DataSet<String> allLines = null;
			int cellIndexOffset = 0;
			for (String path : inputPaths) {
	
				DataSource<String> lines = env.readTextFile(path).name("Load " + path);
	
				if (allLines == null) {
					allLines = lines;
				} else {
					allLines = allLines.union(lines);
				}
	
			}
			
			DataSet<String> taggedSentences = allLines;
			
//			if (!parameters.alreadyTagged) {
//			
//				DataSet<String> cleanSentences = allLines.map(new ReplaceNewLines()).name("Replacing new lines");
//				
//				DataSet<String> splittedSentences = cleanSentences.flatMap(new SplitSentences()).name("Splitting sentences");
//				
//				taggedSentences = splittedSentences.map(new TagSentences()).name("NER-Tagging sentences");
//			
//			}
			
	        //Filter sentences: retain only those that contain both entity tags: <sentence>
			sentencesWithTags = taggedSentences.filter(new FilterByTags(task_entityTags)).name("Filtering out lines by NER tags");

		}
		
		if (parameters.step == 0) {			
			sentencesWithTags.writeAsText(parameters.output+"/sentencesWithTags", FileSystem.WriteMode.OVERWRITE);
		}
		
		if (parameters.step == 1) {
		
	        //Generate a mapping <organization, sentence>
			DataSet<Tuple2<String,String>> organizationSentenceTuples = sentencesWithTags.flatMap(new ExtractOrganizationSentenceTuples()).name("Extracting Orgainization Sentence Tuples");
	
	        //Read the seed tuples as pairs: <organization, location>
			DataSet<Tuple2<String,String>> seedTuples = env.readTextFile(parameters.seedTuples).map(new MapSeedTuplesFromStrings());
	
	        //####################
	        //#START OF ITERATION#
	        //####################
	
	        //Retain only those sentences with a organization from the seed tuples: <<organization, sentence>, <organization, location>>
	        DataSet<Tuple2<Tuple2<String,String>, Tuple2<String,String>>> organizationKeyListJoined = organizationSentenceTuples.joinWithTiny(seedTuples).where(0).equalTo(0).name("Joining Tuple/Sentence Pairs with Seed Tuples to filter out unnecessary sentences");
	        
	        //Search the sentences for raw patterns
	        DataSet<TupleContext> rawPatterns = organizationKeyListJoined.flatMap(new SearchRawPatterns(task_entityTags)).name("Search the sentences for raw patterns");
	
	        rawPatterns.writeAsCsv(parameters.output+"/rawPatterns", FileSystem.WriteMode.OVERWRITE);
		}
		
		if (parameters.step == 2) {
	        DataSource<Tuple5<String, String, String, String, String>> rawPatterns = env.readCsvFile(parameters.inputFile).types(String.class, String.class, String.class, String.class, String.class);
	        
	        DataSet<Tuple5<Map,String,Map,String,Map>> rawPatternsMapped = rawPatterns.map(new RawPatternsMapper());
//	        
//	        rawPatternsMapped.writeAsText(parameters.output+"/tmp", FileSystem.WriteMode.OVERWRITE);
	        
	        //Cluster the raw patterns in a partition
	        DataSet<Tuple2<Tuple5<Map, String, Map, String, Map>, Integer>> clusterCentroids = rawPatternsMapped.mapPartition(new ClusterPartition(parameters.similarityThreshold)).name("Cluster the raw patterns in a partition");
	        
	        clusterCentroids.writeAsCsv(parameters.output+"/clusterCentroids", FileSystem.WriteMode.OVERWRITE);
		}
		
		if  (parameters.step == 3) {
			DataSource<Tuple6<String, String, String, String, String, Integer>> clusterCentroids = env.readCsvFile(parameters.inputFile).types(String.class, String.class, String.class, String.class, String.class, Integer.class);
			
			DataSet<Tuple2<Tuple5<Map, String, Map, String, Map>, Integer>> clusterCentroidsMapped = clusterCentroids.map(new ClusterCentroidsMapper());
	
	        //Cluster the centroids from all partitions
	        DataSet<Tuple5<Map,String,Map,String,Map>> finalPatterns = clusterCentroidsMapped.reduceGroup(new ClusterCentroids(parameters.similarityThreshold, parameters.minimalClusterSize)).name("Cluster the cluster centroids");
	        
	        finalPatterns.writeAsCsv(parameters.output+"/finalPatterns", FileSystem.WriteMode.OVERWRITE);
		}
		
		if (parameters.step == 4) {
			sentencesWithTags = env.readFileOfPrimitives(parameters.inputFile, String.class);
			
		    //Search sentences for occurrences of the two entity tags
		    //Returns: List of <tuple, context>
		    DataSet<Tuple2<Tuple2<String, String>, TupleContext>> textSegments = sentencesWithTags.flatMap(new SearchForTagOccurences(task_entityTags, parameters.maxDistance, parameters.windowSize)).name("Create tuple contexts for found occurences of both NER tags");

		    textSegments.writeAsCsv(parameters.output+"/textSegments", FileSystem.WriteMode.OVERWRITE);
		}
		
		if (parameters.step == 5) {
			
			DataSet<Tuple2<String,String>> seedTuples = env.readTextFile(parameters.seedTuples).map(new MapSeedTuplesFromStrings());
			
			DataSource<Tuple5<String, String, String, String, String>> finalPatternsCsv = env.readCsvFile(parameters.inputFile).types(String.class, String.class, String.class, String.class, String.class);
			
			DataSet<Tuple5<Map, String, Map, String, Map>> finalPatterns = finalPatternsCsv.map(new RawPatternsMapper());
			
			DataSource<Tuple7<String, String, String, String, String, String, String>> textSegments = env.readCsvFile(parameters.inputFile2).types(String.class, String.class, String.class, String.class, String.class, String.class, String.class);

			DataSet<Tuple2<Tuple2<String, String>, Tuple5<Map, String, Map, String, Map>>> textSegmentsMapped = textSegments.map(new TextSegmentMapper());
			
	        //######## Generate pattern confidences
		    //Generate <organization, <pattern_id, location>> when the pattern generated the tuple
		    DataSet<Tuple2<String, Tuple2<Integer, String>>> organizationsWithMatchedLocations = textSegmentsMapped.flatMap(new TupleGenerationPatternsFinder(parameters.degreeOfMatchThreshold)).withBroadcastSet(finalPatterns, "finalPatterns").name("Find patterns that generated those tuples");

		    //Join location from seed tuples onto the matched locations: <<organization, <pattern_id, matched_location>>, <organization, seedtuple_location>>
	        DataSet<Tuple2<Tuple2<String,Tuple2<Integer,String>>,Tuple2<String,String>>> organizationsWithMatchedAndCorrectLocation = organizationsWithMatchedLocations.joinWithTiny(seedTuples).where(0).equalTo(0).name("Join location from seed tuples onto candidate tuples");

	        //Return counts of positives and negatives depending on whether matched location equals seed tuple location: <pattern_id, <#positives, #negatives>>
	        DataSet<Tuple2<Integer, Tuple2<Integer, Integer>>> patternsWithPositiveAndNegatives = organizationsWithMatchedAndCorrectLocation.map(new MapPositivesAndNegatives()).name("Return counts of positives and negatives depending on whether matched location equals seed tuple location");

	        //Sum up counts of positives and negatives for each pattern id: <pattern_id, <#positives, #negatives>>
	        DataSet<Tuple2<Integer, Tuple2<Integer, Integer>>> patternsWithSummedUpPositiveAndNegatives = patternsWithPositiveAndNegatives.groupBy(0).reduce(new ReducePositivesAndNegatives()).name("Sum up counts of positives and negatives for each pattern id");

	        //Calculate pattern confidence: <pattern_id, confidence>
	        DataSet<Tuple2<Integer, Double>> patternConfidences = patternsWithSummedUpPositiveAndNegatives.map(new CalculatePatternConfidences()).name("Calculate pattern confidence");

	        //######## Find occurences of patterns in text
	        //Compile candidate tuple list: <pattern_id, <candidate tuple, similarity>>
	        DataSet<Tuple2<Integer, Tuple2<Tuple2<String, String>, Double>>> patternsWithTuples = textSegmentsMapped.flatMap(new CalculateBestPatternSimilarity(parameters.degreeOfMatchThreshold)).withBroadcastSet(finalPatterns, "finalPatterns").name("Calculate the similarity of the best pattern for each candidate tuple");

	        //######## Make candidate tuples
	        //Join candidate tuples with pattern confidences: <<pattern_id, <candidate tuple, similarity>>, <pattern_id, pattern_conf>>
	        DataSet<Tuple2<Tuple2<Integer,Tuple2<Tuple2<String, String>,Double>>,Tuple2<Integer,Double>>> candidateTuplesWithPatternConfidences = patternsWithTuples.joinWithTiny(patternConfidences).where(0).equalTo(0).name("Join candidate tuples with pattern confidences");

	        //Reformat to <candidate tuple, <pattern_conf, similarity>>
	        DataSet<Tuple2<Tuple2<String, String>, Tuple2<Double, Double>>> candidateTuples = candidateTuplesWithPatternConfidences.map(new CandidateTupleSimplifier()).name("Reformat to <candidate tuple, <pattern_conf, similarity>>");
	        
	        candidateTuples.writeAsCsv(parameters.output+"/candidateTuples", FileSystem.WriteMode.OVERWRITE);
		}
		
		if (parameters.step == 6) {
			DataSource<Tuple4<String, String, String, String>> candidateTuplesCsv = env.readCsvFile(parameters.inputFile).types(String.class, String.class, String.class, String.class);

			DataSet<Tuple2<Tuple2<String, String>, Tuple2<Double, Double>>> candidateTuples = candidateTuplesCsv.map(new CandidateTuplesMapper());
			
			
	        //Execute tuple confidence calculation: <organization, <location, tuple confidence>>
	        DataSet<Tuple2<String, Tuple2<String, Double>>> candidateTupleconfidencesWithOrganizationAsKey = candidateTuples.groupBy(0).reduceGroup(new CandidateTupleConfidenceCalculator()).name("Calculate candidate tuple confidences and use organization as key");

	        //Filter candidate tuples by their confidence: <organization, <location, tuple confidence>>
	        DataSet<Tuple2<String, Tuple2<String, Double>>> filteredTuples = candidateTupleconfidencesWithOrganizationAsKey.filter(new CandidateTupleConfidenceFilter(parameters.tupleConfidenceThreshold)).name("Filter candidate tuples by their confidence");

	        //Filter candidate tuples by organization, choosing highest confidence: <organization, <location, tuple confidence>>
	        DataSet<Tuple2<String, Tuple2<String, Double>>> uniqueFilteredTuples = filteredTuples.groupBy(0).reduceGroup(new UniqueOrganizationReducer()).name("Choose unique location for each organization based on highest confidence");

	        //Store new seed tuples without their confidence: <organization, location>
	        DataSet<Tuple2<String, String>> newSeedTuples = uniqueFilteredTuples.map(new SeedTuplesExtractor()).name("Store new seed tuples without their confidence");
	        
	        newSeedTuples.writeAsCsv(parameters.output+"/newSeedTuples", FileSystem.WriteMode.OVERWRITE);
		}
		
		if (parameters.step == 7) {
			DataSet<Tuple2<String,String>> seedTuples = env.readTextFile(parameters.seedTuples).map(new MapSeedTuplesFromStrings());

			DataSource<Tuple2<String, String>> newSeedTuples = env.readCsvFile(parameters.inputFile).types(String.class, String.class);
			
	        DataSet<Tuple2<String, String>> mergedSeedTuples = seedTuples.union(newSeedTuples).distinct().name("Merge new seed tuples into seed tuples");
	        
	        mergedSeedTuples.writeAsCsv(parameters.output+"/mergedSeedTuples", FileSystem.WriteMode.OVERWRITE);
		}
//
//        DataSet<Tuple2<String, String>> countedMergedSeedTuples = mergedSeedTuples.map(new CountSeedTuples()).name("Count merged seed tuples");
//
//        DataSet<Tuple2<String, String>> resultingSeedTuples = seedTuples.closeWith(countedMergedSeedTuples);
//
//        //##################
//        //#END OF ITERATION#
//        //##################
//
//        //System.out.println("Total tuples:" + resultingSeedTuples.count());
//        resultingSeedTuples.writeAsText(parameters.output, FileSystem.WriteMode.OVERWRITE);

		// Trigger the job execution and measure the execution time.
		long startTime = System.currentTimeMillis();
        try {
            JobExecutionResult results = env.execute("Snowball");
            Integer i;
//            System.out.println("Iteration n: raw patterns => centroids => final patterns => candidate tuples => " +
//                    "(new) seed tuples => final seed tuples");
//            for (i = 1; i <= parameters.numberOfIterations; i++) {
//                String output = "Iteration " + i + ": " + results.getAccumulatorResult("numRawPatterns" + i) + " => " +
//                        results.getAccumulatorResult("numCentroids" + i) + " => " +
//                        results.getAccumulatorResult("numFinalPatterns" + i) + " => " +
//                        results.getAccumulatorResult("numCandidateTuples" + i) + " => " +
//                        results.getAccumulatorResult("numNewSeedTuples" + i) + " => " +
//                        results.getAccumulatorResult("numFinalSeedTuples" + i);
//                System.out.println(output);
//            }
//            for (i = 1; i <= parameters.numberOfIterations; i++) {
//                System.out.println("Iteration " + i + " Histograms:");
//                System.out.println("Cluster Similarity: " + results.getAccumulatorResult("histClusterSimilarities" + i));
//                System.out.println("Match Similarity: " + results.getAccumulatorResult("histMatchSimilarities" + i));
//                System.out.println("Tuple Confidences: " + results.getAccumulatorResult("histTupleConfidences" + i));
//            }
        } finally {
            RemoteCollectorImpl.shutdownAll();
        }
		long endTime = System.currentTimeMillis();
		System.out.format("Execution finished after %.3f s.\n", (endTime - startTime) / 1000d);
	}

    /**
	 * Creates a execution environment as specified by the parameters.
	 */
	private ExecutionEnvironment createExecutionEnvironment() {
        ExecutionEnvironment executionEnvironment;

        if (this.parameters.executor != null) {
            // If a remote executor is explicitly specified, connect.
			final String[] hostAndPort = this.parameters.executor.split(":");
			final String host = hostAndPort[0];
			final int port = Integer.parseInt(hostAndPort[1]);
			if (this.parameters.jars == null || this.parameters.jars.isEmpty()) {
				throw new IllegalStateException("No jars specified to be deployed for remote execution.");
			}
			final String[] jars = new String[this.parameters.jars.size()];
			this.parameters.jars.toArray(jars);
			executionEnvironment = ExecutionEnvironment.createRemoteEnvironment(host, port, jars);

		} else {
            // Otherwise, create a default execution environment.
            executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        }

        // Set the default parallelism explicitly, if requested.
        if (this.parameters.parallelism != -1) {
            executionEnvironment.setParallelism(this.parameters.parallelism);
        }

        return executionEnvironment;
	}

	/**
	 * Collect the input paths from the parameters and expand path patterns.
	 */
	private Collection<String> loadInputPaths() {
		try {
			Collection<String> allInputPaths = new LinkedList<String>();
			for (String rawInputPath : this.parameters.inputFiles) {
				if (rawInputPath.contains("*")) {
					// If the last path of the pattern contains an asterisk, expand the path.
					// Check that the asterisk is contained in the last path segment.
					int lastSlashPos = rawInputPath.lastIndexOf('/');
					if (rawInputPath.indexOf('*') < lastSlashPos) {
						throw new RuntimeException("Path expansion is only possible on the last path segment: " + rawInputPath);
					}

					// Collect all children of the to-be-expanded path.
					String lastSegmentRegex = rawInputPath.substring(lastSlashPos + 1)
							.replace(".", "\\.")
							.replace("[", "\\[")
							.replace("]", "\\]")
							.replace("(", "\\(")
							.replace(")", "\\)")
							.replace("*", ".*");
					Path parentPath = new Path(rawInputPath.substring(0, lastSlashPos));
					FileSystem fs = parentPath.getFileSystem();
					for (FileStatus candidate : fs.listStatus(parentPath)) {
						if (candidate.getPath().getName().matches(lastSegmentRegex)) {
							allInputPaths.add(candidate.getPath().toString());
						}
					}

				} else {
					// Simply add normal paths.
					allInputPaths.add(rawInputPath);
				}
			}
			return allInputPaths;

		} catch (IOException e) {
			throw new RuntimeException("Could not expand paths.", e);
		}
	}

	/**
	 * Parameters for Snowball.
	 */
	private static class Parameters {

		/**
		 * Create parameters from the given command line.
		 */
		static Parameters parse(String... args) {
			try {
				Parameters parameters = new Parameters();
				new JCommander(parameters, args);
				return parameters;
			} catch (final ParameterException e) {
				System.err.println(e.getMessage());
				StringBuilder sb = new StringBuilder();
				new JCommander(new Parameters()).usage(sb);
				for (String line : sb.toString().split("\n")) {
					System.out.println(line);
				}
				System.exit(1);
				return null;
			}
		}

		//Parameters
		
		@Parameter(names = "--step", description = "Step of algorithm", required = true)
		public int step;

		@Parameter(names = "--inputFile", description = "Input file", required = true)
		public String inputFile;
		
		@Parameter(names = "--inputFile2", description = "Second input file", required = false)
		public String inputFile2;
		
		@Parameter(names = "--alreadyTagged", description = "Are the input files already tagged?", required = true)
		public boolean alreadyTagged;
		
		@Parameter(names = "--seedTuples", description = "Seed tuple file", required = true)
		public String seedTuples;

		@Parameter(names = "--iterations", description = "Number of Snowball iterations", required = true)
		public int numberOfIterations;

		@Parameter(names = "--windowSize", description = "Maximum size of the left window (left of first entity tag) and the right window (right of second entity tag)", required = true)
		public int windowSize;

		@Parameter(names = "--maxDistance", description = "Maximum distance between both entity tags in tokens", required = true)
		public int maxDistance;

		@Parameter(names = "--similarityThreshold", description = "Similarity threshold for clustering of patterns (range 0.0-1.0)", required = true)
		public double similarityThreshold;

		@Parameter(names = "--degreeOfMatchThreshold", description = "Minimal degree of match for a pattern to match a text segment (range 0.0-1.0)", required = true)
		public double degreeOfMatchThreshold;

		@Parameter(names = "--minimalClusterSize", description = "Minimal size of cluster", required = true)
		public int minimalClusterSize;

		@Parameter(names = "--tupleConfidenceThreshold", description = "Threshold for tuple confidence (range 0.0-1.0)", required = true)
		public double tupleConfidenceThreshold;

		@Parameter(names = "--output", description = "Output file", required = true)
		public String output;

		@Parameter(description = "input tsv files", required = true)
		public List<String> inputFiles = new ArrayList<String>();

        @Parameter(names = "--parallelism", description = "degree of parallelism for the job execution")
        public int parallelism = -1;

        @Parameter(names = "--jars", description = "set of jars that are relevant to the execution of SINDY")
        public List<String> jars = null;

        @Parameter(names = "--executor", description = "<host name>:<port> of the Flink cluster")
        public String executor = null;

        @Parameter(names = "--distinct-attribute-groups", description = "whether to use only distinct attribute groups")
        public boolean isUseDistinctAttributeGroups = false;
	}
}
