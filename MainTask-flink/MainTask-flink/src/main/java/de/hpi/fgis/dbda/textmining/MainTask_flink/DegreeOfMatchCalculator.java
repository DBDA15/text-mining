package de.hpi.fgis.dbda.textmining.MainTask_flink;

import java.util.List;
import java.util.Map;

public class DegreeOfMatchCalculator {

    public static Double calculateDegreeOfMatch(TupleContext pattern, TupleContext tuple) {
        Map<String, Double> centroidLeft = tuple.f0;
        Map<String, Double> centroidMiddle = tuple.f2;
        Map<String, Double> centroidRight = tuple.f4;

        Map<String, Double> patternLeft = pattern.f0;
        Map<String, Double> patternMiddle = pattern.f2;
        Map<String, Double> patternRight = pattern.f4;

        if (pattern.f1.equals(tuple.f1) && pattern.f3.equals(tuple.f3)) {
            double leftSimilarity = 0;
            double middleSimilarity = 0;
            double rightSimilarity = 0;

            int centroidLeftSum = 0;
            int centroidMiddleSum = 0;
            int centroidRightSum = 0;

            int patternLeftSum = 0;
            int patternMiddleSum = 0;
            int patternRightSum = 0;

            for (String key : patternLeft.keySet()) {
                if (centroidLeft.keySet().contains(key)) {
                    leftSimilarity += patternLeft.get(key) * centroidLeft.get(key);
                    centroidLeftSum += Math.pow(centroidLeft.get(key), 2);
                    patternLeftSum += Math.pow(patternLeft.get(key), 2);
                }
            }
            for (String key : patternMiddle.keySet()) {
                if (centroidMiddle.keySet().contains(key)) {
                    middleSimilarity += patternMiddle.get(key) * centroidMiddle.get(key);
                    centroidMiddleSum += Math.pow(centroidMiddle.get(key), 2);
                    patternMiddleSum += Math.pow(patternMiddle.get(key), 2);
                }
            }
            for (String key : patternRight.keySet()) {
                if (centroidRight.keySet().contains(key)) {
                    rightSimilarity += patternRight.get(key) * centroidRight.get(key);
                    centroidRightSum += Math.pow(centroidRight.get(key), 2);
                    patternRightSum += Math.pow(patternRight.get(key), 2);
                }
            }
            return (leftSimilarity / (Math.sqrt(centroidLeftSum) * Math.sqrt(patternLeftSum)))
                    + (middleSimilarity / (Math.sqrt(centroidMiddleSum) * Math.sqrt(patternMiddleSum)))
                    + (rightSimilarity / (Math.sqrt(centroidRightSum) * Math.sqrt(patternRightSum)));

            //TODO: normalisieren: https://en.wikipedia.org/wiki/Cosine_similarity
        } else {
            return 0.0;
        }
    }
    
    public static Double calculateDegreeOfMatchWithCluster(TupleContext pattern, List<TupleContext> cluster) {
        TupleContext centroid = CentroidCalculator.calculateCentroid(cluster);
        return DegreeOfMatchCalculator.calculateDegreeOfMatch(pattern, centroid);
    }
}
