package de.hpi.fgis.dbda.textmining.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import de.hpi.fgis.dbda.textmining.MainTask_flink.DegreeOfMatchCalculator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.TupleContext;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;

@ForwardedFields("f0->f1.f0")
public class CalculateBestPatternSimilarity extends RichFlatMapFunction<Tuple2<Tuple2<String, String>, Tuple5<Map, String, Map, String, Map>>, Tuple2<Integer, Tuple2<Tuple2<String, String>, Double>>> {

	private double degreeOfMatchThreshold;
	private List<Tuple5<Map, String, Map, String, Map>> patterns;

	public CalculateBestPatternSimilarity(double degreeOfMatchThreshold) {
		this.degreeOfMatchThreshold = degreeOfMatchThreshold;
	}

    @Override
    public void open(Configuration parameters) throws Exception {
        this.patterns = getRuntimeContext().getBroadcastVariable("finalPatterns");
    }

	@Override
	public void flatMap(Tuple2<Tuple2<String, String>, Tuple5<Map, String, Map, String, Map>> textSegment, Collector<Tuple2<Integer, Tuple2<Tuple2<String, String>, Double>>> arg1)
		throws Exception {

        //Algorithm from figure 4

        Tuple2<String, String> candidateTuple = textSegment.f0;
        Tuple5<Map, String, Map, String, Map> tupleContext = textSegment.f1;

        Integer bestPattern = null;
        Double bestSimilarity = 0.0;
        Integer patternIndex = 0;
        while (patternIndex < patterns.size()) {
        	Tuple5<Map, String, Map, String, Map> pattern = patterns.get(patternIndex);
            Double similarity = DegreeOfMatchCalculator.calculateDegreeOfMatch(tupleContext, pattern);
            if (similarity >= degreeOfMatchThreshold) {
            	if (similarity > bestSimilarity) {
            		bestSimilarity = similarity;
                    bestPattern = patternIndex;
                }
            }
            patternIndex++;
        }
        if (bestSimilarity >= degreeOfMatchThreshold) {
            arg1.collect(new Tuple2(bestPattern, new Tuple2(candidateTuple, bestSimilarity)));
        }
    }
}
