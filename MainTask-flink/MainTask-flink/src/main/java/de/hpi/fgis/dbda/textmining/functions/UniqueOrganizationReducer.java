package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;

@ForwardedFields("*->*")
public class UniqueOrganizationReducer implements	ReduceFunction<Tuple2<String, Tuple2<String, Double>>> {

    @Override
    public Tuple2<String, Tuple2<String, Double>> reduce(Tuple2<String, Tuple2<String, Double>> tuple1, Tuple2<String, Tuple2<String, Double>> tuple2) throws Exception {
        return tuple1.f1.f1 > tuple2.f1.f1 ? tuple1 : tuple2;
    }
}
