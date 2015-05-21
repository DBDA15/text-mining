package de.hpi.fgis.dbda.textmining.maintask;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

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
import edu.stanford.nlp.util.CoreMap;

public class App
{

    private static String classifierPath = "/english.all.3class.distsim.crf.ser";
    private static String classifierPropPath = "/english.all.3class.distsim.prop";

    private static transient AbstractSequenceClassifier<? extends CoreMap> classifier = null;

    public static void main( String[] args )
    {
    	
        final String lineItemFile = args[0];
        final String outputFile = args[1];

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
			
			context.addFile(App.class.getResource(classifierPath).getFile());
			context.addFile(App.class.getResource(classifierPropPath).getFile());

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
