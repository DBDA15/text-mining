package de.hpi.fgis.dbda.textmining.functions;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * This function filters empty IND sets.
 *
 * @author sebastian.kruse
 * @since 05.06.2015
 */
@FunctionAnnotation.ReadFields("1")
@FunctionAnnotation.ConstantFields({"0", "1"})
public class FilterEmptyIndSets implements FilterFunction<Tuple2<Integer, int[]>> {

	@Override
	public boolean filter(Tuple2<Integer, int[]> indSet) throws Exception {
		return indSet.f1.length > 0;
	}
}
