package de.hpi.fgis.dbda.textmining.functions;

import de.hpi.fgis.dbda.textmining.MainTask_flink.SeedTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;

@ForwardedFields("f0.f1.f0->f0")
public class MapPositivesAndNegatives implements
		MapFunction<Tuple2<Tuple2<String,Tuple2<Integer,String>>,Tuple2<String,String>>, Tuple2<Integer, Tuple2<Integer, Integer>>> {

	//input: <<organization, <pattern_id, matched_location>>, <organization, seedtuple_location>>
	@Override
	public Tuple2<Integer, Tuple2<Integer, Integer>> map(Tuple2<Tuple2<String,Tuple2<Integer,String>>,Tuple2<String,String>> organizationWithMatchedAndCorrectLocation) throws Exception {
		Integer patternID = organizationWithMatchedAndCorrectLocation.f0.f1.f0;
		String matchedLocation = organizationWithMatchedAndCorrectLocation.f0.f1.f1;
		String correctLocation = organizationWithMatchedAndCorrectLocation.f1.f1;
		if (matchedLocation.equals(correctLocation)) {
			return new Tuple2(patternID, new Tuple2<>(1, 0));
		} else {
			return new Tuple2(patternID, new Tuple2<>(0, 1));
		}
	}

}
