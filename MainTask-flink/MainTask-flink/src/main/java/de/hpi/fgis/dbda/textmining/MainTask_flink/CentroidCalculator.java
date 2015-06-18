package de.hpi.fgis.dbda.textmining.MainTask_flink;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple5;

public class CentroidCalculator {

    public static TupleContext calculateCentroid(List<TupleContext> patterns) {
        Map<String, Double> leftCounter = new LinkedHashMap();
        Map<String, Double>  middleCounter = new LinkedHashMap();
        Map<String, Double>  rightCounter = new LinkedHashMap();

        String leftEntity = patterns.get(0).f1;
        String rightEntity = patterns.get(0).f3;

        //Add up all contexts
        for (TupleContext pattern : patterns) {
            leftCounter = sumMaps(leftCounter, pattern.f0);
            middleCounter = sumMaps(middleCounter, pattern.f2);
            rightCounter = sumMaps(rightCounter, pattern.f4);
        }

        //Normalize counters
        double leftSum = sumCollection(leftCounter.values());
        double middleSum = sumCollection(middleCounter.values());
        double rightSum = sumCollection(rightCounter.values());

        for (String key : leftCounter.keySet()) {
            leftCounter.put(key, leftCounter.get(key) / leftSum);
        }
        for (String key : middleCounter.keySet()) {
            middleCounter.put(key, middleCounter.get(key) / middleSum);
        }
        for (String key : rightCounter.keySet()) {
            rightCounter.put(key, rightCounter.get(key) / rightSum);
        }

        return new TupleContext(leftCounter, leftEntity, middleCounter, rightEntity, rightCounter);
    }

	 private static double sumCollection(Collection<Double> col) {
		 double sum = 0.0;
	     for (double o : col) {
	    	 sum += o;
	     }
	     return sum;
	 }
	 
	 private static Map<String, Double> sumMaps(Map<String, Double> map1, Map<String, Double> map2) {
		 //Add all values of map2 to map1
	     for (Map.Entry<String, Double> entry : map2.entrySet()) {
	    	 if (map1.containsKey(entry.getKey())) {
	    		 map1.put(entry.getKey(), map1.get(entry.getKey()) + entry.getValue());
	    	 } else {
	    		 map1.put(entry.getKey(), entry.getValue());
	    	 }
	     }
	     return map1;
	 }
	
}
