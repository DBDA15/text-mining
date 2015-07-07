package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;

@ForwardedFields("f0; f1.f0->f1")
public class SeedTuplesExtractor
		extends
		RichMapFunction<Tuple2<String, Tuple2<String, Double>>, Tuple2<String, String>> {

	private IntCounter numNewSeedTuples;
	@Override
	public void open(Configuration parameters) throws Exception {
		numNewSeedTuples = new IntCounter();
		getRuntimeContext().addAccumulator("numNewSeedTuples" + getIterationRuntimeContext().getSuperstepNumber(), numNewSeedTuples);
	}

	@Override
	public Tuple2<String, String> map(Tuple2<String, Tuple2<String, Double>> arg0)
			throws Exception {
                String organization = arg0.f0;
                String location = arg0.f1.f0;
				numNewSeedTuples.add(1);
                return new Tuple2(organization, location);
	}

}
