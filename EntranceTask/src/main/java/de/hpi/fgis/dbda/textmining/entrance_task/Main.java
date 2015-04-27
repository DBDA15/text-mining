package de.hpi.fgis.dbda.textmining.entrance_task;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;

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
                    .textFile("data/short.tsv")
                    .map(
                            new Function<String, String>() {
                                public String call(String line) {
                                    if(tagger== null) {
                                        tagger = new MaxentTagger("pos_tagger/taggers/english-left3words-distsim.tagger");
                                    }
                                    LineItem li = new LineItem(line);
                                    String tagged = tagger.tagString(li.TEXT);
                                    //System.out.println(tagged);
                                    return tagged;
                                }
                            });

            lineItems.saveAsTextFile("data/test");
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