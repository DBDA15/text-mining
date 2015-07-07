package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

@ForwardedFields("f0->f0; f1->f1")
public class CountSeedTuples extends RichMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

    private IntCounter numFinalSeedTuples;

    @Override
    public void open(Configuration parameters) throws Exception {
        numFinalSeedTuples = new IntCounter();
        getRuntimeContext().addAccumulator("numFinalSeedTuples" + getIterationRuntimeContext().getSuperstepNumber(), numFinalSeedTuples);
    }

    @Override
    public Tuple2<String, String> map(Tuple2<String, String> seedTuple) throws Exception {
        numFinalSeedTuples.add(1);
        return seedTuple;
    }
}
