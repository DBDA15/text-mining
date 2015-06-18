package de.hpi.fgis.dbda.textmining.MainTask_flink;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple5;

public class CentroidCalculator {

    public static Tuple5<Map, String, Map, String, Map> calculateCentroid(List<Tuple5<Map, String, Map, String, Map>> patterns) {
        Map<String, Float> leftCounter = new LinkedHashMap();
        Map<String, Float>  middleCounter = new LinkedHashMap();
        Map<String, Float>  rightCounter = new LinkedHashMap();

        String leftEntity = patterns.get(0).f1;
        String rightEntity = patterns.get(0).f3;

        //Add up all contexts
        for (Tuple5<Map, String, Map, String, Map> pattern : patterns) {
            leftCounter = sumMaps(leftCounter, pattern.f0);
            middleCounter = sumMaps(middleCounter, pattern.f2);
            rightCounter = sumMaps(rightCounter, pattern.f4);
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

        return new Tuple5<Map, String, Map, String, Map>(leftCounter, leftEntity, middleCounter, rightEntity, rightCounter);
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
