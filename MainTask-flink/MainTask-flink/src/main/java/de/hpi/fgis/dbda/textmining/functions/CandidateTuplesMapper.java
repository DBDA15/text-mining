package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

public class CandidateTuplesMapper implements
		MapFunction<Tuple4<String, String, String, String>, Tuple2<Tuple2<String, String>, Tuple2<Double, Double>>> {

	@Override
	public Tuple2<Tuple2<String, String>, Tuple2<Double, Double>> map(Tuple4<String, String, String, String> value)
			throws Exception {
		String organization = value.f0.substring(1);
		String location = value.f1.substring(0, value.f1.length()-1);
		Double patternConf = Double.parseDouble(value.f2.substring(1));
		Double similarity = Double.parseDouble(value.f3.substring(0, value.f3.length()-1));
		return new Tuple2<Tuple2<String,String>, Tuple2<Double,Double>>(new Tuple2<String, String>(organization, location), new Tuple2<Double, Double>(patternConf, similarity) );
	}

}
