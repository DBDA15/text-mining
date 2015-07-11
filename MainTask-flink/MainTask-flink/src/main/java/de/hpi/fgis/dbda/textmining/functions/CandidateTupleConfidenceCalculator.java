package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

@ForwardedFields("f0.f0->f0; f0.f1->f1.f0")
public class CandidateTupleConfidenceCalculator implements GroupReduceFunction<Tuple2<Tuple2<String, String>, Tuple2<Double, Double>>, Tuple2<String, Tuple2<String, Double>>> {

	@Override
	public void reduce(Iterable<Tuple2<Tuple2<String, String>, Tuple2<Double, Double>>> arg0,
			Collector<Tuple2<String, Tuple2<String, Double>>> arg1) throws Exception {
		
		double tupleConfidenceSubtrahend = 1.0;
		Tuple2<String, String> tuple = null;
		
		for (Tuple2<Tuple2<String, String>, Tuple2<Double, Double>> tupleData : arg0) {
			tupleConfidenceSubtrahend *= (1.0 - ( tupleData.f1.f0 * tupleData.f1.f1 ));
			if (tuple == null) {
				tuple = tupleData.f0;
			}
		}
		arg1.collect(new Tuple2(tuple.f0, new Tuple2(tuple.f1, (1.0 - tupleConfidenceSubtrahend))));
	}

}
