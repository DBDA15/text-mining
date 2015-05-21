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
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.process.DocumentPreprocessor;
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
        final String classifierPath = args[2];
        final String classifierPropPath = args[3];

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
            
            
			JavaRDD<String> splittedSentences = lineItems
					.flatMap(new FlatMapFunction<String, String>() {

						public Iterable<String> call(String t) throws Exception {
							LineItem li = new LineItem(t);
							Reader reader = new StringReader(li.TEXT);
							DocumentPreprocessor dp = new DocumentPreprocessor(reader);
							List<String> sentenceList = new ArrayList<String>();
							for (List<HasWord> sentence : dp) {
								String sentenceString = Sentence.listToString(sentence);
								sentenceList.add(sentenceString.toString());
							}
							return sentenceList;
						}
					});

			splittedSentences.saveAsTextFile(outputFile+"/sentences");
			
			context.addFile(classifierPath);
			context.addFile(classifierPropPath);

			// tag articles
			JavaRDD<String> taggedSentences = splittedSentences
					.map(new Function<String, String>() {
						public String call(String sentence) {							
							if (classifier == null) {
								try {
									classifier = CRFClassifier.getClassifier(SparkFiles.get(classifierPath));
								} catch (IOException e) {
									return "IOException";
								} catch (ClassNotFoundException e) {
									return "ClassNotFoundException";
								}
							}
							String tagged = classifier.classifyToString(sentence);
							return tagged;
						}
					});

            taggedSentences.saveAsTextFile(outputFile+"/tagged");

			JavaRDD<Tuple5> rawPatterns = taggedSentences
                    .flatMap(new FlatMapFunction<String, Tuple5>() {
                        @Override
                        public Iterable<Tuple5> call(String sentence) throws Exception {
                            List<Tuple5> patterns = new ArrayList<Tuple5>();
                            String[] splittedSentence = sentence.split(" ");

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
                                    if (word == seedTuple._1() && entity == task_entityTags.get(0)) {
                                        entity0sites.add(wordIndex);
                                    } else if (word == seedTuple._2() && entity == task_entityTags.get(1)) {
                                        entity1sites.add(wordIndex);
                                    }

                                    wordIndex++;
                                }

                                for (Integer entity0site : entity0sites) {
                                    for (Integer entity1site : entity1sites) {
                                        if (entity0site < entity1site) {
                                            String beforeContext = StringUtils.join(words.subList(Math.max(0, entity0site - 5), entity0site), " ");
                                            String betweenContext = StringUtils.join(words.subList(entity0site + 1, entity1site), " ");
                                            String afterContext = StringUtils.join(words.subList(entity1site + 1, Math.min(words.size(), entity1site + 5)), " ");
                                            Tuple5 pattern = new Tuple5(beforeContext, task_entityTags.get(0), betweenContext, task_entityTags.get(1), afterContext);
                                            patterns.add(pattern);
                                        } else {
                                            String beforeContext = StringUtils.join(words.subList(Math.max(0, entity1site - 5), entity1site), " ");
                                            String betweenContext = StringUtils.join(words.subList(entity1site + 1, entity0site), " ");
                                            String afterContext = StringUtils.join(words.subList(entity0site + 1, Math.min(words.size(), entity0site + 5)), " ");
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
