package de.hpi.fgis.dbda.textmining.functions;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class RawPatternsMapper
		implements MapFunction<Tuple5<String, String, String, String, String>, Tuple5<Map, String, Map, String, Map>> {

	@Override
	public Tuple5<Map, String, Map, String, Map> map(Tuple5<String, String, String, String, String> value)
			throws Exception {
		Map<String, Double> leftMap = ContextMapper.mapContext(value.f0);
		Map<String, Double> middleMap = ContextMapper.mapContext(value.f2);
		Map<String, Double> rightMap = ContextMapper.mapContext(value.f4);
		return new Tuple5<Map, String, Map, String, Map>(leftMap, value.f1, middleMap, value.f3, rightMap);
	}

}
