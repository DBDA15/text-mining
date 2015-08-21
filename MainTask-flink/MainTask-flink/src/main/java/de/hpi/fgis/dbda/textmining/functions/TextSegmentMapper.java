package de.hpi.fgis.dbda.textmining.functions;

import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;

public class TextSegmentMapper implements
		MapFunction<Tuple7<String, String, String, String, String, String, String>, Tuple2<Tuple2<String, String>, Tuple5<Map, String, Map, String, Map>>> {

	@Override
	public Tuple2<Tuple2<String, String>, Tuple5<Map, String, Map, String, Map>> map(
			Tuple7<String, String, String, String, String, String, String> value) throws Exception {
		String organization = value.f0.substring(1);
		String location = value.f1.substring(0, value.f1.length()-1);
		Map<String, Double> leftMap = ContextMapper.mapContext(value.f2);
		Map<String, Double> middleMap = ContextMapper.mapContext(value.f4);
		Map<String, Double> rightMap = ContextMapper.mapContext(value.f6);
		Tuple2<String, String> orgLocTuple = new Tuple2<String, String>(organization, location);
		Tuple5<Map, String, Map, String, Map> contextTuple = new Tuple5<Map, String, Map, String, Map>(leftMap, value.f3, middleMap, value.f5, rightMap);
		return new Tuple2<Tuple2<String,String>, Tuple5<Map,String,Map,String,Map>>(orgLocTuple,contextTuple);
	}

}
