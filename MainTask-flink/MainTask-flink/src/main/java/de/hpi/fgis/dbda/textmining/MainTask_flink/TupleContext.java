package de.hpi.fgis.dbda.textmining.MainTask_flink;

import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple5;

public class TupleContext extends Tuple5<Map, String, Map, String, Map> {

	public TupleContext(Map _1, String _2, Map _3, String _4, Map _5) {
		super(_1, _2, _3, _4, _5);
	}

}
