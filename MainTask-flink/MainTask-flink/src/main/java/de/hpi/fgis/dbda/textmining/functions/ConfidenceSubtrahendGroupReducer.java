package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class ConfidenceSubtrahendGroupReducer implements GroupReduceFunction<Tuple2<Tuple2<String, String>, Tuple2<Float, Float>>, Tuple2<Tuple2<String, String>, Float>> {

	@Override
	public void reduce(Iterable<Tuple2<Tuple2<String, String>, Tuple2<Float, Float>>> arg0,
			Collector<Tuple2<Tuple2<String, String>, Float>> arg1) throws Exception {
		
		float tupleConfidence = 1.0f;
		Tuple2 tuple = null;
		
		for (Tuple2<Tuple2<String, String>, Tuple2<Float, Float>> tupleData : arg0) {
			tupleConfidence *= (1.0f - ( tupleData.f1.f0 * tupleData.f1.f1 ));
			if (tuple == null) {
				tuple = tupleData.f0;
			}
		}
		
		arg1.collect(new Tuple2<Tuple2<String, String>, Float>(tuple, 1- tupleConfidence));

	}

}
