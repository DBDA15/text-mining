package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CandidateTupleConfidenceFilter implements FilterFunction<Tuple2<String, Tuple2<String, Float>>> {
	
	float tupleConfidenceThreshold;

	public CandidateTupleConfidenceFilter(float tupleConfidenceThreshold) {
		this.tupleConfidenceThreshold = tupleConfidenceThreshold;
	}

	@Override
	public boolean filter(Tuple2<String, Tuple2<String, Float>> arg0) throws Exception {
		Float confidence = arg0.f1.f1;
        if (confidence > tupleConfidenceThreshold) {
        	return true;
        } else {
        	return false;
        }
    }

}
