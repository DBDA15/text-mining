package de.hpi.fgis.dbda.textmining.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import de.hpi.fgis.dbda.textmining.MainTask_flink.CentroidCalculator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.DegreeOfMatchCalculator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.TupleContext;

public class ClusterCentroids extends RichGroupReduceFunction<Tuple2<TupleContext, Integer>, TupleContext> {

    private double similarityThreshold;
    private int minimalClusterSize;
    private IntCounter numFinalPatterns;

    public ClusterCentroids(double similarityThreshold, int minimalClusterSize) {
        super();
        this.similarityThreshold = similarityThreshold;
        this.minimalClusterSize = minimalClusterSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        numFinalPatterns = new IntCounter();
        getRuntimeContext().addAccumulator("numFinalPatterns" + getIterationRuntimeContext().getSuperstepNumber(), numFinalPatterns);
    }

    @Override
    public void reduce(Iterable<Tuple2<TupleContext, Integer>> centroids, Collector<TupleContext> collector) throws Exception {
        List<Tuple2<List, Integer>> clusters = new ArrayList<>();
        for (Tuple2<TupleContext, Integer> centroidWithSize : centroids) {
            TupleContext centroid = centroidWithSize.f0;
            Integer clusterSize = centroidWithSize.f1;
            if (clusters.isEmpty()) {
                List<TupleContext> newCluster = new ArrayList<>();
                newCluster.add(centroid);
                clusters.add(new Tuple2(newCluster, clusterSize));
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
                    List centroidsInNearestCluster = clusters.get(nearestCluster).f0;
                    centroidsInNearestCluster.add(centroid);
                    clusters.set(nearestCluster, new Tuple2(centroidsInNearestCluster, clusters.get(nearestCluster).f1 + clusterSize));
                } else {
                    List<TupleContext> newCluster = new ArrayList<>();
                    newCluster.add(centroid);
                    clusters.add(new Tuple2(newCluster, clusterSize));
                }
            }
        }
        for (Tuple2<List, Integer> cluster : clusters) {
            //TODO: dynamic cluster size threshold
            if (cluster.f1 > minimalClusterSize) {
                TupleContext centroid = CentroidCalculator.calculateCentroid(cluster.f0);
                this.numFinalPatterns.add(1);
                collector.collect(centroid);
            }
        }
    }
}
