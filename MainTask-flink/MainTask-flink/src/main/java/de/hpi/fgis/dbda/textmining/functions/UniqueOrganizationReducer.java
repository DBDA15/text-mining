package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class UniqueOrganizationReducer implements	GroupReduceFunction<Tuple2<String, Tuple2<String, Double>>, Tuple2<String, Tuple2<String, Double>>> {

	@Override
	public void reduce(Iterable<Tuple2<String, Tuple2<String, Double>>> arg0, Collector<Tuple2<String, Tuple2<String, Double>>> arg1) throws Exception {
		
		Tuple2<String, Tuple2<String, Double>> bestOrganization = null;
		for (Tuple2<String, Tuple2<String, Double>> tuple : arg0) {
			if (bestOrganization == null || bestOrganization.f1.f1 < tuple.f1.f1) {
				bestOrganization = tuple;
			}
		}
		arg1.collect(bestOrganization);

	}

}
