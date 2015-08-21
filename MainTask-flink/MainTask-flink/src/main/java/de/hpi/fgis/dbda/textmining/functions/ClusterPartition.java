package de.hpi.fgis.dbda.textmining.functions;

import de.hpi.fgis.dbda.textmining.MainTask_flink.CentroidCalculator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.DegreeOfMatchCalculator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.TupleContext;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ClusterPartition extends RichMapPartitionFunction<Tuple5<Map, String, Map, String, Map>, Tuple2<Tuple5<Map, String, Map, String, Map>, Integer>> {

    private double similarityThreshold;

    public ClusterPartition(double similarityThreshold) {
        super();
        this.similarityThreshold = similarityThreshold;
    }

    @Override
    public void mapPartition(Iterable<Tuple5<Map, String, Map, String, Map>> rawPatterns, Collector<Tuple2<Tuple5<Map, String, Map, String, Map>, Integer>> collector) throws Exception {
        List<List> clusters = new ArrayList<>();
        for (Tuple5<Map, String, Map, String, Map> pattern : rawPatterns) {
            if (clusters.isEmpty()) {
                List<Tuple5<Map, String, Map, String, Map>> newCluster = new ArrayList<>();
                newCluster.add(pattern);
                clusters.add(newCluster);
            } else {
                Integer clusterIndex = 0;
                Integer nearestCluster = null;
                Double greatestSimilarity = 0.0;
                for (List<Tuple5<Map, String, Map, String, Map>> cluster : clusters) {
                    Double similarity = DegreeOfMatchCalculator.calculateDegreeOfMatchWithCluster(pattern, cluster);
                    if (similarity > greatestSimilarity) {
                        nearestCluster = clusterIndex;
                        greatestSimilarity = similarity;
                    }
                    clusterIndex++;
                }

                if (greatestSimilarity > similarityThreshold) {
                    clusters.get(nearestCluster).add(pattern);
                } else {
                    List<Tuple5<Map, String, Map, String, Map>> separateCluster = new ArrayList<>();
                    separateCluster.add(pattern);
                    clusters.add(separateCluster);
                }
            }
        }
        for (List<Tuple5<Map, String, Map, String, Map>> cluster : clusters) {
            //TODO: dynamic cluster size threshold
        	Tuple5<Map, String, Map, String, Map> centroid = CentroidCalculator.calculateCentroid(cluster);
            collector.collect(new Tuple2(centroid, cluster.size()));
        }
    }
}
