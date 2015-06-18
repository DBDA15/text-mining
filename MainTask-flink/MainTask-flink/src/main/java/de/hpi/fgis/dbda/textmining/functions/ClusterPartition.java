package de.hpi.fgis.dbda.textmining.functions;

import de.hpi.fgis.dbda.textmining.MainTask_flink.CentroidCalculator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.DegreeOfMatchCalculator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.TupleContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by eldarion on 18/06/15.
 */
public class ClusterPartition implements org.apache.flink.api.common.functions.MapPartitionFunction<TupleContext, Tuple2<TupleContext, Integer>> {

    private float similarityThreshold;

    public ClusterPartition(float similarityThreshold) {
        super();
        this.similarityThreshold = similarityThreshold;
    }

    @Override
    public void mapPartition(Iterable<TupleContext> rawPatterns, Collector<Tuple2<TupleContext, Integer>> collector) throws Exception {
        List<List> clusters = new ArrayList<>();
        for (TupleContext pattern : rawPatterns) {
            if (clusters.isEmpty()) {
                List<TupleContext> newCluster = new ArrayList<>();
                newCluster.add(pattern);
                clusters.add(newCluster);
            } else {
                Integer clusterIndex = 0;
                Integer nearestCluster = null;
                Float greatestSimilarity = 0.0f;
                for (List<TupleContext> cluster : clusters) {
                    Float similarity = DegreeOfMatchCalculator.calculateDegreeOfMatchWithCluster(pattern, cluster);
                    if (similarity > greatestSimilarity) {
                        nearestCluster = clusterIndex;
                        greatestSimilarity = similarity;
                    }
                    clusterIndex++;
                }

                if (greatestSimilarity > similarityThreshold) {
                    clusters.get(nearestCluster).add(pattern);
                } else {
                    List<TupleContext> separateCluster = new ArrayList<>();
                    separateCluster.add(pattern);
                    clusters.add(separateCluster);
                }
            }
        }
        for (List<TupleContext> cluster : clusters) {
            //TODO: dynamic cluster size threshold
            TupleContext centroid = CentroidCalculator.calculateCentroid(cluster);
            collector.collect(new Tuple2(centroid, cluster.size()));
        }
    }
}
