package de.hpi.fgis.dbda.textmining.entrance_task;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple1;
import scala.Tuple2;

public class Main
{
    static transient MaxentTagger tagger = null;

    public static void main( String[] args )
    {

        // String tagged = tagger.tagString(sample);

        // initialize spark environment
        SparkConf config = new SparkConf().setAppName(Main.class.getName());
        config.set("spark.hadoop.validateOutputSpecs", "false");

        try(JavaSparkContext context = new JavaSparkContext(config)) {
            JavaRDD<String> lineItems = context
                    .textFile("data/short.tsv");
            //tag articles
            JavaRDD<String> taggedLines = lineItems
                    .map(
                            new Function<String, String>() {
                                public String call(String line) {
                                    if (tagger == null) {
                                        tagger = new MaxentTagger("pos_tagger/taggers/english-left3words-distsim.tagger");
                                    }
                                    LineItem li = new LineItem(line);
                                    String tagged = tagger.tagString(li.TEXT);
                                    //System.out.println(tagged);
                                    return tagged;
                                }
                            });
            //split tagged articles into words
            JavaRDD<String> wordTags = taggedLines
                    .flatMap(
                            new FlatMapFunction<String, String>() {
                                @Override
                                public Iterable<String> call(String tagLine) throws Exception {
                                    String[] splitTagLine = tagLine.split(" ");
                                    List<String> splitTagLineList = Arrays.asList(splitTagLine);
                                    return splitTagLineList;
                                }
                            }
                    );
            //generate Pairs of <word, <tag, 1>>
            JavaPairRDD<String, Tuple2> pairs = wordTags
                    .mapToPair(
                            new PairFunction<String, String, Tuple2>() {
                                @Override
                                public Tuple2<String, Tuple2> call(String wordTag) throws Exception {
                                    String[] splitWordTag = wordTag.split("_");
                                    Tuple2 tagAndInt = new Tuple2<>(splitWordTag[1], 1);
                                    return new Tuple2<>(splitWordTag[0], tagAndInt);
                                }
                            }
                    );
            pairs.saveAsTextFile("data/test");
        }
    }

    static class LineItem implements Serializable {
        Integer INDEX;
        String TEXT;

        LineItem(String line) {
            String[] values = line.split("\t");
            INDEX = Integer.parseInt(values[0]);
            TEXT = values[4];
        }
    }
}