package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;

@ForwardedFields("*->*")
public class UniqueOrganizationReducer implements	GroupReduceFunction<Tuple2<String, Tuple2<String, Double>>, Tuple2<String, Tuple2<String, Double>>> {

	@Override
	public void reduce(Iterable<Tuple2<String, Tuple2<String, Double>>> arg0, Collector<Tuple2<String, Tuple2<String, Double>>> arg1) throws Exception {
		
		Tuple2<String, Tuple2<String, Double>> bestLocation = null;
		for (Tuple2<String, Tuple2<String, Double>> tuple : arg0) {
			if (bestLocation == null || bestLocation.f1.f1 < tuple.f1.f1) {
				bestLocation = tuple;
			}
		}
		arg1.collect(bestLocation);
	}

}
