package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CandidateTupleSimplifier
		implements
		MapFunction<Tuple2<Tuple2<Integer, Tuple2<Tuple2, Float>>, Tuple2<Integer, Float>>, Tuple2<Tuple2, Tuple2<Float, Float>>> {

	@Override
	public Tuple2<Tuple2, Tuple2<Float, Float>> map(Tuple2<Tuple2<Integer, Tuple2<Tuple2, Float>>, Tuple2<Integer, Float>> arg0) throws Exception {
        Tuple2 candidateTuple = arg0.f0.f1.f0;
        Float patternConf = arg0.f1.f1;
        Float similarity = arg0.f0.f1.f1;
        return new Tuple2(candidateTuple, new Tuple2(patternConf, similarity));
	}

}
