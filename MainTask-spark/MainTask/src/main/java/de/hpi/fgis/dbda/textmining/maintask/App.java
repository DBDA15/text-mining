package de.hpi.fgis.dbda.textmining.maintask;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import scala.Tuple5;
import scala.Tuple2;
import edu.stanford.nlp.util.CoreMap;

public class App
{

    private static transient AbstractSequenceClassifier<? extends CoreMap> classifier = null;

    public static void main( String[] args )
    {
    	
        final String lineItemFile = args[0];
        final String outputFile = args[1];

        final List<String> task_entityTags = new ArrayList<>();
        task_entityTags.add("ORGANIZATION");
        task_entityTags.add("LOCATION");

        final List<Tuple2> task_seedTuples = new ArrayList<>();
        task_seedTuples.add(new Tuple2("Microsoft", "Redmond"));
        task_seedTuples.add(new Tuple2("Parliament", "Emirates"));

        // initialize spark environment
        SparkConf config = new SparkConf().setAppName(App.class.getName());
        config.set("spark.hadoop.validateOutputSpecs", "false");

        try(JavaSparkContext context = new JavaSparkContext(config)) {
            JavaRDD<String> lineItems = context
                    .textFile(lineItemFile);

			JavaRDD<Tuple5> rawPatterns = lineItems
                    .flatMap(new FlatMapFunction<String, Tuple5>() {
                        @Override
                        public Iterable<Tuple5> call(String sentence) throws Exception {
                            List<Tuple5> patterns = new ArrayList<Tuple5>();
                            String[] splittedSentence = sentence.split(" ");
                            //System.out.println(sentence);

                            Integer tupleIndex = 0;
                            for (Tuple2 seedTuple : task_seedTuples) {
                                List<String> words = new ArrayList<String>();
                                List<Integer> entity0sites = new ArrayList<Integer>();
                                List<Integer> entity1sites = new ArrayList<Integer>();

                                Integer wordIndex = 0;
                                for (String wordEntity : splittedSentence) {
                                    String[] splitted = wordEntity.split("/");
                                    String word = splitted[0];
                                    String entity = splitted[1];

                                    words.add(word);
                                    if (word.equals(seedTuple._1()) && entity.equals(task_entityTags.get(0))) {
                                        entity0sites.add(wordIndex);
                                    } else if (word.equals(seedTuple._2()) && entity.equals(task_entityTags.get(1))) {
                                        entity1sites.add(wordIndex);
                                    }

                                    wordIndex++;
                                }

                                for (Integer entity0site : entity0sites) {
                                    for (Integer entity1site : entity1sites) {
                                        Integer windowSize = 5;
                                        if (entity0site < entity1site) {
                                            String beforeContext = StringUtils.join(words.subList(Math.max(0, entity0site - windowSize), entity0site), " ");
                                            String betweenContext = StringUtils.join(words.subList(entity0site + 1, entity1site), " ");
                                            String afterContext = StringUtils.join(words.subList(entity1site + 1, Math.min(words.size(), entity1site + windowSize + 1)), " ");
                                            Tuple5 pattern = new Tuple5(beforeContext, task_entityTags.get(0), betweenContext, task_entityTags.get(1), afterContext);
                                            patterns.add(pattern);
                                        } else {
                                            String beforeContext = StringUtils.join(words.subList(Math.max(0, entity1site - windowSize), entity1site), " ");
                                            String betweenContext = StringUtils.join(words.subList(entity1site + 1, entity0site), " ");
                                            String afterContext = StringUtils.join(words.subList(entity0site + 1, Math.min(words.size(), entity0site + windowSize + 1)), " ");
                                            Tuple5 pattern = new Tuple5(beforeContext, task_entityTags.get(1), betweenContext, task_entityTags.get(0), afterContext);
                                            patterns.add(pattern);
                                        }
                                    }
                                }
                            }

                            return patterns;
                        }
                    });

            rawPatterns.saveAsTextFile(outputFile+"/patterns");
            System.out.println("Fertisch!");

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
