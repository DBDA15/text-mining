package de.hpi.fgis.dbda.textmining.functions;

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * This function merges cells with the same value by calculating the union of their indexes.
 *
 * @author sebastian.kruse
 * @since 05.06.2015
 */
@RichGroupReduceFunction.Combinable
public class MergeCells extends RichGroupReduceFunction<Tuple2<int[], String>, Tuple2<int[], String>> {

	private final IntSortedSet aggregator = new IntRBTreeSet();

	@Override
	public void reduce(Iterable<Tuple2<int[], String>> cells, Collector<Tuple2<int[], String>> out) throws Exception {
		// Collect all indexes that we find.
		Tuple2<int[], String> lastCell = null;
		for (Tuple2<int[], String> cell : cells) {
			for (int cellIndex : cell.f0) {
				this.aggregator.add(cellIndex);
			}
			lastCell = cell;
		}

		// Write out the merged cell with all indexes.
		lastCell.f0 = this.aggregator.toIntArray();
		out.collect(lastCell);
		this.aggregator.clear();
	}
}
