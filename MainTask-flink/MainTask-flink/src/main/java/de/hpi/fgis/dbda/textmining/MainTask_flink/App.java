package de.hpi.fgis.dbda.textmining.MainTask_flink;


import de.hpi.fgis.dbda.textmining.functions.*;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.LocalExecutor;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

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
		
		DataSet<String> taggedSentences = allLines;
		
		DataSet<String> cleanSentences = allLines.map(new ReplaceNewLines()).name("Replacing new lines");
			
		DataSet<String> splittedSentences = cleanSentences.flatMap(new SplitSentences()).name("Splitting sentences");
			
		taggedSentences = splittedSentences.map(new TagSentences()).name("NER-Tagging sentences");
		
		taggedSentences.writeAsText(parameters.output, FileSystem.WriteMode.OVERWRITE);
		
        // Trigger the job execution and measure the execution time.
		long startTime = System.currentTimeMillis();
        try {
            JobExecutionResult results = env.execute("Snowball");
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
