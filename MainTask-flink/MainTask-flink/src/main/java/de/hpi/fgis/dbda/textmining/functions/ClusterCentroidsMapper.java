package de.hpi.fgis.dbda.textmining.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

public class ClusterCentroidsMapper implements
		MapFunction<Tuple6<String, String, String, String, String, Integer>, Tuple2<Tuple5<Map, String, Map, String, Map>, Integer>> {

	@Override
	public Tuple2<Tuple5<Map, String, Map, String, Map>, Integer> map(
			Tuple6<String, String, String, String, String, Integer> value) throws Exception {
		Map<String, Double> leftMap = ContextMapper.mapContext(value.f0);
		Map<String, Double> middleMap = ContextMapper.mapContext(value.f2);
		Map<String, Double> rightMap = ContextMapper.mapContext(value.f4);
		return new Tuple2<Tuple5<Map,String,Map,String,Map>, Integer>(new Tuple5<Map, String, Map, String, Map>(leftMap, value.f1, middleMap, value.f3, rightMap), value.f5);
	}

}
