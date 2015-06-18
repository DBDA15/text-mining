package de.hpi.fgis.dbda.textmining.MainTask_flink;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;

public class ContextProducer {

	public static Map produceContext(List<Tuple2> tokenList) {
        /*
           Produce the context based on a given token list. A context is a HashMap that maps each token in the token
           list to a weight. The more prominent or frequent a token is, the higher is the associated weight.
         */
        Map<String, Integer> termCounts = new LinkedHashMap();

        //Count how often each token occurs
        Integer sumCounts = 0;
        for (Tuple2<String, String> token : tokenList) {
            if (!termCounts.containsKey(token.f0)) {
                termCounts.put(token.f0, 1);
            } else {
                termCounts.put(token.f0, termCounts.get(token.f0) + 1);
            }
            sumCounts += 1;
        }

        //Calculate token frequencies out of the counts
        Map<String, Double> context = new LinkedHashMap();
        for (Map.Entry<String, Integer> entry : termCounts.entrySet()) {
            context.put(entry.getKey(), (double) entry.getValue() / sumCounts);
        }
        return context;
    }

	
}
