package de.hpi.fgis.dbda.textmining.functions;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import de.hpi.fgis.dbda.textmining.MainTask_flink.DegreeOfMatchCalculator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.TupleContext;

@ForwardedFields("f0.f0->f0; f0.f1->f1.f1")
public class TupleGenerationPatternsFinder extends RichFlatMapFunction<Tuple2<Tuple2<String, String>, Tuple5<Map, String, Map, String, Map>>, Tuple2<String, Tuple2<Integer, String>>> {

	private double degreeOfMatchThreshold;
	private List<Tuple5<Map, String, Map, String, Map>> finalPatterns;
	
	public TupleGenerationPatternsFinder(double degreeOfMatchThreshold) {
		super();
		this.degreeOfMatchThreshold = degreeOfMatchThreshold;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		this.finalPatterns = getRuntimeContext().getBroadcastVariable("finalPatterns");
	}


	@Override
	public void flatMap(Tuple2<Tuple2<String, String>, Tuple5<Map, String, Map, String, Map>> textSegment,
			Collector<Tuple2<String, Tuple2<Integer, String>>> arg1)
			throws Exception {
		Tuple5<Map, String, Map, String, Map> tupleContext = textSegment.f1;

        Integer patternIndex = 0;
        while (patternIndex < finalPatterns.size()) {
        	Tuple5<Map, String, Map, String, Map> pattern = finalPatterns.get(patternIndex);
			Double similarity = DegreeOfMatchCalculator.calculateDegreeOfMatch(tupleContext, pattern);
            if (similarity >= degreeOfMatchThreshold) {
				arg1.collect(new Tuple2(textSegment.f0.f0, new Tuple2(patternIndex, textSegment.f0.f1)));
            }
            patternIndex++;
        }
	}

}
