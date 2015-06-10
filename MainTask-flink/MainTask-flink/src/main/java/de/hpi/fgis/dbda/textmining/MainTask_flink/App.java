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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/** Implementation of the SINDY algorithm for scalable IND discovery. */
public class App {

	private static final int ATTRIBUTE_INDEX_OFFSET = 1000;

	/** Stores execution parameters of this job. */
	private final Parameters parameters;

	/** Maps attribute indexes to files. */
	private Int2ObjectMap<String> filesByAttributeIndexOffset;

	public static void main(String[] args) throws Exception {
		App sindy = new App(args);
		sindy.run();
	}

	public App(String[] args) {
		this.parameters = Parameters.parse(args);
	}

	private void run() throws Exception {
		// Load the execution environment.
		final ExecutionEnvironment env = createExecutionEnvironment();

		// Read and parse the input files.
		this.filesByAttributeIndexOffset = new Int2ObjectOpenHashMap<>();
		Collection<String> inputPaths = loadInputPaths();
		DataSet<Tuple2<int[], String>> cells = null;
		int cellIndexOffset = 0;
		for (String path : inputPaths) {

			DataSource<String> lines = env.readTextFile(path).name("Load " + path);

			DataSet<Tuple2<int[], String>> fileCells = lines
					.flatMap(new CreateCells(';', cellIndexOffset))
					.name("Parse " + path);

			this.filesByAttributeIndexOffset.put(cellIndexOffset, path);
			cellIndexOffset += ATTRIBUTE_INDEX_OFFSET;

			if (cells == null) {
				cells = fileCells;
			} else {
				cells = cells.union(fileCells);
			}

		}

		// Join the cells and keep the attribute groups.
		DataSet<Tuple1<int[]>> attributeGroups = cells
				.groupBy(1).reduceGroup(new MergeCells())
				.name("Merge cells")
				.project(0);

		// If requested, remove duplicate attribute groups.
		if (this.parameters.isUseDistinctAttributeGroups) {
			attributeGroups = attributeGroups
					.map(new ConvertIntArrayToString()).name("Encode attribute groups as Base64")
					.distinct()
					.map(new CovertStringToIntArray()).name("Decode attribute groups from Base64");
		}

		// Create IND evidences from the attribute groups.
		DataSet<Tuple2<Integer, int[]>> indEvidences = attributeGroups.flatMap(new CreateIndEvidences());

		// Merge the IND evidences to actual INDs.
		DataSet<Tuple2<Integer, int[]>> inds = indEvidences
				.groupBy(0).reduceGroup(new MergeIndEvidences())
				.name("Merge IND evidences")
				.filter(new FilterEmptyIndSets())
				.name("Filter empty IND sets");

		// Have the INDs sent to the drivers and print them to the stdout.
		collectAndPrintInds(inds);

		// Trigger the job execution and measure the exeuction time.
		long startTime = System.currentTimeMillis();
        try {
            env.execute("SINDY");
        } finally {
            RemoteCollectorImpl.shutdownAll();
        }
		long endTime = System.currentTimeMillis();
		System.out.format("Exection finished after %.3f s.\n", (endTime - startTime) / 1000d);
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
            // Otherwise, create a default exection environment.
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
	 * Parameters for SINDY.
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

		@Parameter(description = "Number of Snowball iterations", required = true)
		public int numberOfIterations;

		@Parameter(description = "Maximum size of the left window (left of first entity tag) and the right window (right of second entity tag)", required = true)
		public int windowSize;

		@Parameter(description = "Maximum distance between both entity tags in tokens", required = true)
		public int maxDistance;

		@Parameter(description = "Similarity threshold for clustering of patterns", required = true)
		public float similarityThreshold;

		@Parameter(description = "Minimal degree of match for a pattern to match a text segment", required = true)
		public float degreeOfMatchThreshold;

		@Parameter(description = "Maximum distance between both entity tags in tokens", required = true)
		public int maxDistance;

		@Parameter(description = "Minimal size of cluster", required = true)
		public int minimalClusterSize;

		@Parameter(description = "Threshold for tuple confidence", required = true)
		public float tupleConfidenceThreshold;

		@Parameter(description = "input tsv files", required = true)
		public List<String> inputFiles = new ArrayList<String>();
	}
}
