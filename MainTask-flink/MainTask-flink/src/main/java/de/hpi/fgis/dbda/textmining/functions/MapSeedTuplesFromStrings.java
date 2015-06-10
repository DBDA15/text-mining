package de.hpi.fgis.dbda.textmining.functions;

import java.io.Serializable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import de.hpi.fgis.dbda.textmining.MainTask_flink.SeedTuple;


public class MapSeedTuplesFromStrings implements
		MapFunction<String, Tuple2<String, String>> {

	@Override
	public Tuple2<String, String> map(String arg0) throws Exception {
		SeedTuple st = new SeedTuple(arg0);
		return new Tuple2<String, String>(st.ORGANIZATION, st.LOCATION);
	}

}
