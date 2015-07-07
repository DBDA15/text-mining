package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class CandidateTupleSimplifier
		extends
		RichMapFunction<Tuple2<Tuple2<Integer, Tuple2<Tuple2<String, String>, Double>>, Tuple2<Integer, Double>>, Tuple2<Tuple2<String, String>, Tuple2<Double, Double>>> {

	private IntCounter numCandidateTuples;

	@Override
	public void open(Configuration parameters) throws Exception {
		numCandidateTuples = new IntCounter();
		getRuntimeContext().addAccumulator("numCandidateTuples" + getIterationRuntimeContext().getSuperstepNumber(), numCandidateTuples);
	}

	@Override
	public Tuple2<Tuple2<String, String>, Tuple2<Double, Double>> map(Tuple2<Tuple2<Integer, Tuple2<Tuple2<String, String>, Double>>, Tuple2<Integer, Double>> arg0) throws Exception {
        Tuple2 candidateTuple = arg0.f0.f1.f0;
        Double patternConf = arg0.f1.f1;
		Double similarity = arg0.f0.f1.f1;
		numCandidateTuples.add(1);
        return new Tuple2(candidateTuple, new Tuple2(patternConf, similarity));
	}

}
