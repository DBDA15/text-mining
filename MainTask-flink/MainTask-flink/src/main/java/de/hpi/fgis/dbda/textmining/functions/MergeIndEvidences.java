package de.hpi.fgis.dbda.textmining.functions;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * This function merges IND evidences with the same dependent attribute.
 *
 * @author sebastian.kruse
 * @since 05.06.2015
 */
@RichGroupReduceFunction.Combinable
public class MergeIndEvidences extends RichGroupReduceFunction<Tuple2<Integer, int[]>, Tuple2<Integer, int[]>> {

	private final IntSet aggregator = new IntOpenHashSet();

	@Override
	public void reduce(Iterable<Tuple2<Integer, int[]>> indEvidences, Collector<Tuple2<Integer, int[]>> out) throws Exception {

		// Collect all indexes that we find.
		Tuple2<Integer, int[]> lastEvidence = null;
		for (Tuple2<Integer, int[]> evidence : indEvidences) {
			if (this.aggregator.isEmpty()) {
				for (int attributeIndex : evidence.f1) {
					this.aggregator.add(attributeIndex);
				}
			} else {
				IntList newAttributeIndexes = IntArrayList.wrap(evidence.f1);
				this.aggregator.retainAll(newAttributeIndexes);
				if (this.aggregator.isEmpty()) break;
			}
			lastEvidence = evidence;
		}

		// Write out the merged cell with all indexes.
		if (lastEvidence.f1.length != this.aggregator.size()) {
			lastEvidence.f1 = this.aggregator.toIntArray();
		}
		out.collect(lastEvidence);
		this.aggregator.clear();
	}
}
