package de.hpi.fgis.dbda.textmining.functions;

import de.hpi.fgis.dbda.textmining.MainTask_flink.CentroidCalculator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.DegreeOfMatchCalculator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.TupleContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ClusterCentroids implements org.apache.flink.api.common.functions.MapPartitionFunction<Tuple2<TupleContext, Integer>, TupleContext> {

    private double similarityThreshold;
    private int minimalClusterSize;

    public ClusterCentroids(double similarityThreshold, int minimalClusterSize) {
        super();
        this.similarityThreshold = similarityThreshold;
        this.minimalClusterSize = minimalClusterSize;
    }

    @Override
    public void mapPartition(Iterable<Tuple2<TupleContext, Integer>> centroids, Collector<TupleContext> collector) throws Exception {
        List<Tuple2<List, Integer>> clusters = new ArrayList<>();
        for (Tuple2<TupleContext, Integer> centroidWithSize : centroids) {
            TupleContext centroid = centroidWithSize.f0;
            Integer clusterSize = centroidWithSize.f1;
            if (clusters.isEmpty()) {
                List<TupleContext> centroidList = new ArrayList<>();
                centroidList.add(centroid);
                clusters.add(new Tuple2(centroidList, clusterSize));
            } else {
                Integer clusterIndex = 0;
                Integer nearestCluster = null;
                Double greatestSimilarity = 0.0;
                for (Tuple2<List, Integer> cluster : clusters) {
                    List<TupleContext> currentCentroidList = cluster.f0;
                    Double similarity = DegreeOfMatchCalculator.calculateDegreeOfMatchWithCluster(centroid, currentCentroidList);
                    if (similarity > greatestSimilarity) {
                        nearestCluster = clusterIndex;
                        greatestSimilarity = similarity;
                    }
                    clusterIndex++;
                }

                if (greatestSimilarity > similarityThreshold) {
                    clusters.get(nearestCluster).f0.add(centroid);
                    clusters.get(nearestCluster).f1 += clusterSize;
                } else {
                    List<TupleContext> centroidList = new ArrayList<>();
                    centroidList.add(centroid);
                    clusters.add(new Tuple2(centroidList, clusterSize));
                }
            }
        }
        for (Tuple2<List, Integer> cluster : clusters) {
            //TODO: dynamic cluster size threshold
            if (cluster.f1 > minimalClusterSize) {
                TupleContext centroid = CentroidCalculator.calculateCentroid(cluster.f0);
                collector.collect(centroid);
            }
        }
    }
}
