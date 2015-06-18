package de.hpi.fgis.dbda.textmining.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import de.hpi.fgis.dbda.textmining.MainTask_flink.DegreeOfMatchCalculator;

public class CalculateBestPatternSimilarity implements FlatMapFunction<Tuple2<Tuple2<String, String>, Tuple5<Map, String, Map, String, Map>>, Tuple2<Integer, Tuple2<Tuple2<String, String>, Float>>> {

	private float degreeOfMatchThreshold;
	private List<Tuple5<Map, String, Map, String, Map>> patterns;

	public CalculateBestPatternSimilarity(float degreeOfMatchThreshold,
			List<Tuple5<Map, String, Map, String, Map>> patterns) {
		this.degreeOfMatchThreshold = degreeOfMatchThreshold;
		this.patterns = patterns;
	}

	@Override
	public void flatMap(Tuple2<Tuple2<String, String>, Tuple5<Map, String, Map, String, Map>> arg0, Collector<Tuple2<Integer, Tuple2<Tuple2<String, String>, Float>>> arg1)
		throws Exception {

        //Algorithm from figure 4
        //
        List<Tuple2<Integer, Tuple2<Tuple2<String, String>, Float>>> candidateTuplesWithPatternAndSimilarity = new ArrayList();

        Tuple2<String, String> candidateTuple = arg0.f0;
        Tuple5<Map, String, Map, String, Map> tupleContext = arg0.f1;

        Integer bestPattern = null;
        float bestSimilarity = 0.0f;
        Integer patternIndex = 0;
        while (patternIndex < patterns.size()) {
        	Tuple5<Map, String, Map, String, Map> pattern = patterns.get(patternIndex);
            float similarity = DegreeOfMatchCalculator.calculateDegreeOfMatch(tupleContext, pattern);
            if (similarity >= degreeOfMatchThreshold) {
            	if (similarity > bestSimilarity) {
            		bestSimilarity = similarity;
                    bestPattern = patternIndex;
                }
            }
            patternIndex++;
        }
        if (bestSimilarity >= degreeOfMatchThreshold) {
        	candidateTuplesWithPatternAndSimilarity.add(new Tuple2(bestPattern, new Tuple2(candidateTuple, bestSimilarity)));
        }
                
		for (Tuple2<Integer, Tuple2<Tuple2<String, String>, Float>> tuple : candidateTuplesWithPatternAndSimilarity) {
			arg1.collect(tuple);
        }
    }
}
