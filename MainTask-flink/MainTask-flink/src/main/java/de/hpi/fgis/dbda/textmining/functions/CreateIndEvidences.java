package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * This function creates IND evidences from attribute groups.
 *
 * @author Sebastian Kruse
 */
public class CreateIndEvidences implements FlatMapFunction<Tuple1<int[]>, Tuple2<Integer, int[]>> {

    private final static int[] EMPTY_CANDIDATES = {};

    private final Tuple2<Integer, int[]> outputTuple = new Tuple2<Integer, int[]>();

    @Override
    public void flatMap(final Tuple1<int[]> attributeGroup,
            final Collector<Tuple2<Integer, int[]>> out) throws Exception {

		// Allocate an array for the referenced attribute indexes.
		int[] attributeArray = attributeGroup.f0;
        int[] includingAttributes;
        if (attributeArray.length == 1) {
            includingAttributes = EMPTY_CANDIDATES;
        } else {
            includingAttributes = new int[attributeArray.length - 1];
			System.arraycopy(attributeArray, 1, includingAttributes, 0, includingAttributes.length);
        }

		// Create the first IND evidence.
		this.outputTuple.f0 = attributeArray[0];
		this.outputTuple.f1 = includingAttributes;
		out.collect(this.outputTuple);

		// Iteratively create all other IND evidences.
        for (int attributeIndex = 0; attributeIndex < includingAttributes.length; attributeIndex++) {
			// Exchange the dependent attribute index with the attribute index at attributeIndex.
            final int swap = includingAttributes[attributeIndex];
			includingAttributes[attributeIndex] = this.outputTuple.f0;
			this.outputTuple.f0 = swap;

			// Collect the new evidence.
            out.collect(this.outputTuple);
        }

    }

}
