package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;

@ForwardedFields("f0")
public class ReducePositivesAndNegatives implements
        org.apache.flink.api.common.functions.ReduceFunction<org.apache.flink.api.java.tuple.Tuple2<Integer, org.apache.flink.api.java.tuple.Tuple2<Integer, Integer>>> {
    @Override
    public Tuple2<Integer, Tuple2<Integer, Integer>> reduce(Tuple2<Integer, Tuple2<Integer, Integer>> input1, Tuple2<Integer, Tuple2<Integer, Integer>> input2) throws Exception {
        Integer patternID1 = input1.f0;
        Integer patternID2 = input2.f0;
        assert patternID1 == patternID2;
        Integer sumOfPositives = input1.f1.f0 + input2.f1.f0;
        Integer sumOfNegatives = input1.f1.f1 + input2.f1.f1;
        return new Tuple2(patternID1, new Tuple2(sumOfPositives, sumOfNegatives));
    }
}