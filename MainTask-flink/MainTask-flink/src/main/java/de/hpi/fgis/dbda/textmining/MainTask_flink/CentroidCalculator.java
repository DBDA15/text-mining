package de.hpi.fgis.dbda.textmining.MainTask_flink;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CentroidCalculator {

    public static TupleContext calculateCentroid(List<TupleContext> patterns) {
        Map<String, Float> leftCounter = new LinkedHashMap();
        Map<String, Float>  middleCounter = new LinkedHashMap();
        Map<String, Float>  rightCounter = new LinkedHashMap();

        String leftEntity = patterns.get(0)._2();
        String rightEntity = patterns.get(0)._4();

        //Add up all contexts
        for (TupleContext pattern : patterns) {
            leftCounter = sumMaps(leftCounter, pattern._1());
            middleCounter = sumMaps(middleCounter, pattern._3());
            rightCounter = sumMaps(rightCounter, pattern._5());
        }

        //Normalize counters
        float leftSum = sumCollection(leftCounter.values());
        float middleSum = sumCollection(middleCounter.values());
        float rightSum = sumCollection(rightCounter.values());

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

	 private static float sumCollection(Collection<Float> col) {
		 float sum = 0.0f;
	     for (float o : col) {
	    	 sum += o;
	     }
	     return sum;
	 }
	 
	 private static Map<String, Float> sumMaps(Map<String, Float> map1, Map<String, Float> map2) {
		 //Add all values of map2 to map1
	     for (Map.Entry<String, Float> entry : map2.entrySet()) {
	    	 if (map1.containsKey(entry.getKey())) {
	    		 map1.put(entry.getKey(), map1.get(entry.getKey()) + entry.getValue());
	    	 } else {
	    		 map1.put(entry.getKey(), entry.getValue());
	    	 }
	     }
	     return map1;
	 }
	
}
