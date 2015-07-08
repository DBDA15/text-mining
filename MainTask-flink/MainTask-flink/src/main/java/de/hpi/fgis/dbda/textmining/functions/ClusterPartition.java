package de.hpi.fgis.dbda.textmining.functions;

import de.hpi.fgis.dbda.textmining.MainTask_flink.CentroidCalculator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.DegreeOfMatchCalculator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.TupleContext;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


public class ClusterPartition extends RichMapPartitionFunction<TupleContext, Tuple2<TupleContext, Integer>> {

    private double similarityThreshold;
    private IntCounter numCentroids;
    private Histogram histSimilarity;

    public ClusterPartition(double similarityThreshold) {
        super();
        this.similarityThreshold = similarityThreshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        numCentroids = new IntCounter();
        histSimilarity = new Histogram();
        getRuntimeContext().addAccumulator("numCentroids" + getIterationRuntimeContext().getSuperstepNumber(), numCentroids);
        getRuntimeContext().addAccumulator("histSimilarity" + getIterationRuntimeContext().getSuperstepNumber(), histSimilarity);
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
                Double greatestSimilarity = 0.0;
                for (List<TupleContext> cluster : clusters) {
                    Double similarity = DegreeOfMatchCalculator.calculateDegreeOfMatchWithCluster(pattern, cluster);
                    if (similarity > greatestSimilarity) {
                        nearestCluster = clusterIndex;
                        greatestSimilarity = similarity;
                    }
                    clusterIndex++;
                }

                if (greatestSimilarity > similarityThreshold) {
                    clusters.get(nearestCluster).add(pattern);
                    histSimilarity.add(greatestSimilarity.intValue());
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
            this.numCentroids.add(1);
            collector.collect(new Tuple2(centroid, cluster.size()));
        }
    }
}
