package de.hpi.fgis.dbda.textmining.maintask;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.*;
import edu.stanford.nlp.io.IOUtils;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class App
{

    private static String classifierPath = "ner-tagger/classifiers/english.all.3class.nodistsim.crf.ser.gz";

    private static transient AbstractSequenceClassifier<CoreLabel> classifier = null;

    public static void main( String[] args )
    {

        // initialize spark environment
        SparkConf config = new SparkConf().setAppName(App.class.getName());
        config.set("spark.hadoop.validateOutputSpecs", "false");

        try(JavaSparkContext context = new JavaSparkContext(config)) {
            JavaRDD<String> lineItems = context
                    .textFile("data/short.tsv");

            //tag articles
            JavaRDD<String> taggedLines = lineItems
                    .map(
                            new Function<String, String>() {
                                public String call(String line) {
                                    if (classifier == null) {
                                        try {
                                            classifier = CRFClassifier.getClassifier(classifierPath);
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        } catch (ClassNotFoundException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    LineItem li = new LineItem(line);
                                    String tagged = classifier.classifyToString(li.TEXT);
                                    System.out.println(tagged);
                                    return tagged;
                                }
                            });

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
