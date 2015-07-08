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

            double centroidLeftSum = 0;
            double centroidMiddleSum = 0;
            double centroidRightSum = 0;

            double patternLeftSum = 0;
            double patternMiddleSum = 0;
            double patternRightSum = 0;

            for (String key : patternLeft.keySet()) {
                patternLeftSum += Math.pow(patternLeft.get(key), 2);
                if (centroidLeft.keySet().contains(key)) {
                    leftSimilarity += patternLeft.get(key) * centroidLeft.get(key);
                }
            }
            for (String key : centroidLeft.keySet()) {
                centroidLeftSum += Math.pow(centroidLeft.get(key), 2);
            }

            for (String key : patternMiddle.keySet()) {
                patternMiddleSum += Math.pow(patternMiddle.get(key), 2);
                if (centroidMiddle.keySet().contains(key)) {
                    middleSimilarity += patternMiddle.get(key) * centroidMiddle.get(key);
                }
            }
            for (String key : centroidMiddle.keySet()) {
                centroidMiddleSum += Math.pow(centroidMiddle.get(key), 2);
            }

            for (String key : patternRight.keySet()) {
                patternRightSum += Math.pow(patternRight.get(key), 2);
                if (centroidRight.keySet().contains(key)) {
                    rightSimilarity += patternRight.get(key) * centroidRight.get(key);
                    centroidRightSum += Math.pow(centroidRight.get(key), 2);
                }
            }
            for (String key : centroidRight.keySet()) {
                centroidRightSum += Math.pow(centroidRight.get(key), 2);
            }

            double left, middle, right;

            if (Math.sqrt(centroidLeftSum) > 0.0 || Math.sqrt(patternLeftSum) > 0.0) {
                left = (leftSimilarity / (Math.sqrt(centroidLeftSum) * Math.sqrt(patternLeftSum)));
            } else {
                left = 0.0;
            }

            if (Math.sqrt(centroidMiddleSum) > 0.0 || Math.sqrt(patternMiddleSum) > 0.0) {
                middle = (middleSimilarity / (Math.sqrt(centroidMiddleSum) * Math.sqrt(patternMiddleSum)));
            } else {
                middle = 0.0;
            }

            if (Math.sqrt(centroidRightSum) > 0.0 || Math.sqrt(patternRightSum) > 0.0) {
                right = (rightSimilarity / (Math.sqrt(centroidRightSum) * Math.sqrt(patternRightSum)));
            } else {
                right = 0.0;
            }
            return (left + middle + right) / 3;

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
