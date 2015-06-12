package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.java.tuple.Tuple2;

public class CalculatePatternConfidences implements
        org.apache.flink.api.common.functions.MapFunction<org.apache.flink.api.java.tuple.Tuple2<Integer, org.apache.flink.api.java.tuple.Tuple2<Integer, Integer>>, org.apache.flink.api.java.tuple.Tuple2<Integer, Float>> {
    @Override
    public Tuple2<Integer, Float> map(Tuple2<Integer, Tuple2<Integer, Integer>> patternWithSummedUpPositiveAndNegatives) throws Exception {
        Integer patternID = patternWithSummedUpPositiveAndNegatives.f0;
        Integer positives = patternWithSummedUpPositiveAndNegatives.f1.f0;
        Integer negatives = patternWithSummedUpPositiveAndNegatives.f1.f1;
        Float confidence = (float) positives / (positives + negatives);
        return new Tuple2(patternID, confidence);
    }
}
