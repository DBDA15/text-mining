package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class UniqueOrganizationReducer implements	GroupReduceFunction<Tuple2<String, Tuple2<String, Float>>, Tuple2<String, Tuple2<String, Float>>> {

	@Override
	public void reduce(Iterable<Tuple2<String, Tuple2<String, Float>>> arg0, Collector<Tuple2<String, Tuple2<String, Float>>> arg1) throws Exception {
		
		Tuple2<String, Tuple2<String, Float>> bestOrganization = null;
		for (Tuple2<String, Tuple2<String, Float>> tuple : arg0) {
			if (bestOrganization == null || bestOrganization.f1.f1 < tuple.f1.f1) {
				bestOrganization = tuple;
			}
		}
		arg1.collect(bestOrganization);

	}

}
