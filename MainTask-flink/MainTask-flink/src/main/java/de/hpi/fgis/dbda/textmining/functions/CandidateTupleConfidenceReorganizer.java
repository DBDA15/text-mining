package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CandidateTupleConfidenceReorganizer implements MapFunction<Tuple2<Tuple2<String, String>, Float>, Tuple2<String, Tuple2<String, Float>>> {
			
	@Override
	public Tuple2<String, Tuple2<String, Float>> map(Tuple2<Tuple2<String, String>, Float> arg0) throws Exception {
		String organization = (String) arg0.f0.f0;
	    String location = (String) arg0.f0.f1;
	    Float confidence = arg0.f1;
	    return new Tuple2(organization, new Tuple2(location, confidence));
	}
}
