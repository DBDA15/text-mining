package de.hpi.fgis.dbda.textmining.MainTask_flink;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import de.hpi.fgis.dbda.textmining.functions.*;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.RemoteCollectorConsumer;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin;
import org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Implementation of the SINDY algorithm for scalable IND discovery. */
public class App {

	private static final int ATTRIBUTE_INDEX_OFFSET = 1000;

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
		
		System.out.println("allLines count: " + allLines.count());

        //Filter sentences: retain only those that contain both entity tags
		DataSet<String> sentencesWithTags = allLines.filter(new FilterByTags(task_entityTags));

        //Generate a mapping <organization, sentence>
		DataSet<Tuple2<String,String>> organizationSentenceTuples = sentencesWithTags.flatMap(new ExtractOrganizationSentenceTuples());

		System.out.println("organizationSentenceTuples count: "+organizationSentenceTuples.count());

        //Read the seed tuples as pairs: <organization, location>
		DataSet<Tuple2<String,String>> seedTuples = env.readTextFile(parameters.seedTuples).map(new MapSeedTuplesFromStrings());

        //Retain only those sentences with a organization from the seed tuples: <<organization, sentence>, <organization, location>>
        DataSet<Tuple2<Tuple2<String,String>, Tuple2<String,String>>> organizationKeyListJoined = organizationSentenceTuples.join(seedTuples).where(0).equalTo(0);

        System.out.println("organizationKeyListJoined count: "+organizationKeyListJoined.count());

        //Search the sentences for raw patterns
        DataSet<TupleContext> rawPatterns = organizationKeyListJoined.flatMap(new SearchRawPatterns(task_entityTags));

        System.out.println("Raw patterns count: "+rawPatterns.count());
        rawPatterns.print();

		// Trigger the job execution and measure the execution time.
		long startTime = System.currentTimeMillis();
        try {
            env.execute("Snowball");
        } finally {
            RemoteCollectorImpl.shutdownAll();
        }
		long endTime = System.currentTimeMillis();
		System.out.format("Execution finished after %.3f s.\n", (endTime - startTime) / 1000d);
	}

	private void collectAndPrintInds(DataSet<Tuple2<Integer, int[]>> indSets) {
		RemoteCollectorImpl.collectLocal(indSets, new RemoteCollectorConsumer<Tuple2<Integer, int[]>>() {
			@Override
			public void collect(Tuple2<Integer, int[]> indSet) {
				for (int referencedAttributeIndex : indSet.f1) {
					System.out.format("%s < %s\n", makeAttributeIndexHumanReadable(indSet.f0),
							makeAttributeIndexHumanReadable(referencedAttributeIndex));
				}
			}
		});
	}

    public static List<scala.Tuple2> generateTokenList(String sentence) {
        //Create regex pattern that finds NER XML tags in the sentence (e.g. "<LOCATION>New York</LOCATION>")
        Pattern NERTagPattern = Pattern.compile("<([A-Z]+)>(.+?)</([A-Z]+)>");
        Matcher NERMatcher = NERTagPattern.matcher(sentence);

        //Store all tokens in a list of 2-tuples <string, NER tag>
        List<scala.Tuple2> tokenList = new ArrayList();
        Integer lastIndex = 0;
        //Iterate through all occurences of the regex pattern
        while (NERMatcher.find()) {
            //First, add the normal words (i.e. w/o NER tags) to the token list
            //Add them as 2-tuples <string, "">
            String stringBefore = sentence.substring(lastIndex, NERMatcher.start());
            String[] splittedStringBefore = stringBefore.split(" ");
            for (String word : splittedStringBefore) {
                if (!word.isEmpty()) {
                    tokenList.add(new scala.Tuple2(word, ""));
                }
            }

            //Then, add the NER-tagged tokens to the token list
            //Add them as 2 tuples <string, NER tag>
            tokenList.add(new scala.Tuple2(NERMatcher.group(2), NERMatcher.group(1)));

            //Remember last processed character
            lastIndex = NERMatcher.end();
        }
        //Lastly, add the normal words (i.e. w/o NER tags) after the last NER tag
        //Add them as 2-tuples <string, "">
        String endString = sentence.substring(lastIndex, sentence.length());
        String[] splittedEndString = endString.split(" ");
        for (String word : splittedEndString) {
            if (!word.isEmpty()) {
                tokenList.add(new scala.Tuple2(word, ""));
            }
        }
        return tokenList;
    }

    public static Map produceContext(List<scala.Tuple2> tokenList) {
        /*
           Produce the context based on a given token list. A context is a HashMap that maps each token in the token
           list to a weight. The more prominent or frequent a token is, the higher is the associated weight.
         */
        Map<String, Integer> termCounts = new LinkedHashMap();

        //Count how often each token occurs
        Integer sumCounts = 0;
        for (scala.Tuple2<String, String> token : tokenList) {
            if (!termCounts.containsKey(token._1())) {
                termCounts.put(token._1(), 1);
            } else {
                termCounts.put(token._1(), termCounts.get(token._1()) + 1);
            }
            sumCounts += 1;
        }

        //Calculate token frequencies out of the counts
        Map<String, Float> context = new LinkedHashMap();
        for (Map.Entry<String, Integer> entry : termCounts.entrySet()) {
            context.put(entry.getKey(), (float) entry.getValue() / sumCounts);
        }
        return context;
    }

	/** Converts an attribute index into the form file[index]. */
	private String makeAttributeIndexHumanReadable(int attributeIndex) {
		int fileLocalIndex = attributeIndex % ATTRIBUTE_INDEX_OFFSET;
		int fileAttributeIndex = attributeIndex - fileLocalIndex;
		return String.format("%s[%d]", this.filesByAttributeIndexOffset.get(fileAttributeIndex), fileLocalIndex);
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
            executionEnvironment.setDegreeOfParallelism(this.parameters.parallelism);
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

		@Parameter(names = "--seedTuples", description = "Seed tuple file", required = true)
		public String seedTuples;

		@Parameter(names = "--iterations", description = "Number of Snowball iterations", required = true)
		public int numberOfIterations;

		@Parameter(names = "--windowSize", description = "Maximum size of the left window (left of first entity tag) and the right window (right of second entity tag)", required = true)
		public int windowSize;

		@Parameter(names = "--maxDistance", description = "Maximum distance between both entity tags in tokens", required = true)
		public int maxDistance;

		@Parameter(names = "--similarityThreshold", description = "Similarity threshold for clustering of patterns", required = true)
		public float similarityThreshold;

		@Parameter(names = "--degreeOfMatchThreshold", description = "Minimal degree of match for a pattern to match a text segment", required = true)
		public float degreeOfMatchThreshold;

		@Parameter(names = "--minimalClusterSize", description = "Minimal size of cluster", required = true)
		public int minimalClusterSize;

		@Parameter(names = "--tupleConfidenceThreshold", description = "Threshold for tuple confidence", required = true)
		public float tupleConfidenceThreshold;

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
