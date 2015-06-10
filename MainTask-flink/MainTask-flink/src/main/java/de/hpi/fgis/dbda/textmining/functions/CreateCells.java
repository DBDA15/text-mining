package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Splits a CSV line into cells.
 *
 * @author sebastian.kruse
 * @since 05.06.2015
 */
public class CreateCells implements FlatMapFunction<String, Tuple2<int[], String>> {

	private final int cellIndexOffset;

	private final String splitChar;

	private final Tuple2<int[], String> outputTuple = new Tuple2<int[], String>(new int[1], null);

	/**
	 * Creates a new instance of this function.
	 * @param splitChar is the CSV field separator
	 * @param cellIndexOffset is the index of the cells in the first field of CSV rows
	 */
	public CreateCells(char splitChar, int cellIndexOffset) {
		this.splitChar = String.valueOf(splitChar);
		this.cellIndexOffset = cellIndexOffset;
	}

	@Override
	public void flatMap(String line, Collector<Tuple2<int[], String>> out) throws Exception {
		String[] fields = line.split(this.splitChar);
		this.outputTuple.f0[0] = this.cellIndexOffset;
		for (String field : fields) {
			this.outputTuple.f1 = field;
			out.collect(this.outputTuple);
			this.outputTuple.f0[0]++;
		}
	}
}
