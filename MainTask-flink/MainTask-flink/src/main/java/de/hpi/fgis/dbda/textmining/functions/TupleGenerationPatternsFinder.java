package de.hpi.fgis.dbda.textmining.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import de.hpi.fgis.dbda.textmining.MainTask_flink.DegreeOfMatchCalculator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.TupleContext;

public class TupleGenerationPatternsFinder extends RichFlatMapFunction<Tuple2<Tuple2<String, String>, TupleContext>, Tuple2<String, Tuple2<Integer, String>>> {

	private double degreeOfMatchThreshold;
	private List<TupleContext> patterns;
	
	public TupleGenerationPatternsFinder(double degreeOfMatchThreshold) {
		super();
		this.degreeOfMatchThreshold = degreeOfMatchThreshold;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.patterns = getRuntimeContext().getBroadcastVariable("finalPatterns");
	}

	@Override
	public void flatMap(Tuple2<Tuple2<String, String>, TupleContext> arg0,
			Collector<Tuple2<String, Tuple2<Integer, String>>> arg1)
			throws Exception {
		List<Tuple2<String, Tuple2<Integer, String>>> generatedTuples = new ArrayList();
        TupleContext tupleContext = arg0.f1;

        Integer patternIndex = 0;
        while (patternIndex < patterns.size()) {
            TupleContext pattern = patterns.get(patternIndex);
			Double similarity = DegreeOfMatchCalculator.calculateDegreeOfMatch(tupleContext, pattern);
            if (similarity >= degreeOfMatchThreshold) {
                generatedTuples.add(new Tuple2(arg0.f0.f0, new Tuple2(patternIndex, arg0.f0.f1)));
            }
            patternIndex++;
        }
        // <organization, <pattern_id, location>>
		for (Tuple2<String, Tuple2<Integer, String>> tupleWithPatternId : generatedTuples) {
        	arg1.collect(tupleWithPatternId);
        }
		
	}

}
