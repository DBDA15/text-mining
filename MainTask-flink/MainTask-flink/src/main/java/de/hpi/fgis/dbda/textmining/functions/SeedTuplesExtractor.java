package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class SeedTuplesExtractor
		implements
		MapFunction<Tuple2<String, Tuple2<String, Double>>, Tuple2<String, String>> {

	@Override
	public Tuple2<String, String> map(Tuple2<String, Tuple2<String, Double>> arg0)
			throws Exception {
                String organization = arg0.f0;
                String location = arg0.f1.f0;
                return new Tuple2(organization, location);
	}

}
