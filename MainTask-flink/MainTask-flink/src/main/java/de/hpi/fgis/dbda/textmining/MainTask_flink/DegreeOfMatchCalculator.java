package de.hpi.fgis.dbda.textmining.MainTask_flink;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple5;

public class DegreeOfMatchCalculator {

    public static Float calculateDegreeOfMatch(Tuple5<Map, String, Map, String, Map> pattern, Tuple5<Map, String, Map, String, Map> tuple) {
        Map<String, Float> centroidLeft = tuple.f0;
        Map<String, Float> centroidMiddle = tuple.f2;
        Map<String, Float> centroidRight = tuple.f4;

        Map<String, Float> patternLeft = pattern.f0;
        Map<String, Float> patternMiddle = pattern.f2;
        Map<String, Float> patternRight = pattern.f4;

        if (pattern.f1.equals(tuple.f1) && pattern.f3.equals(tuple.f3)) {
            float leftSimilarity = 0;
            float middleSimilarity = 0;
            float rightSimilarity = 0;
            for (String key : patternLeft.keySet()) {
                if (centroidLeft.keySet().contains(key)) {
                    leftSimilarity += patternLeft.get(key) * centroidLeft.get(key);
                }
            }
            for (String key : patternMiddle.keySet()) {
                if (centroidMiddle.keySet().contains(key)) {
                    middleSimilarity += patternMiddle.get(key) * centroidMiddle.get(key);
                }
            }
            for (String key : patternRight.keySet()) {
                if (centroidRight.keySet().contains(key)) {
                    rightSimilarity += patternRight.get(key) * centroidRight.get(key);
                }
            }
            return leftSimilarity + middleSimilarity + rightSimilarity;

            //TODO: normalisieren: https://en.wikipedia.org/wiki/Cosine_similarity
        } else {
            return 0.0f;
        }
    }
    
    public static Float calculateDegreeOfMatchWithCluster(Tuple5<Map, String, Map, String, Map> pattern, List<Tuple5<Map, String, Map, String, Map>> cluster) {
        Tuple5<Map, String, Map, String, Map> centroid = CentroidCalculator.calculateCentroid(cluster);
        return DegreeOfMatchCalculator.calculateDegreeOfMatch(pattern, centroid);
    }
}
