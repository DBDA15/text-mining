package de.hpi.fgis.dbda.textmining.maintask;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.util.Time;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import de.hpi.fgis.dbda.textmining.maintask.Tagging.LineItem;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.process.DocumentPreprocessor;
import scala.Tuple2;

public class App
{
	
	private final static String classifierPath = "edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz";
	private static transient AbstractSequenceClassifier<CoreLabel> classifier = null;

    public static void main( String[] args )
    {
    	
    	long startTime = Time.now();

        final String outputDirectory = args[0];

        //Initialize spark environment
        SparkConf config = new SparkConf().setAppName(App.class.getName());
        config.set("spark.hadoop.validateOutputSpecs", "false");

        try(JavaSparkContext context = new JavaSparkContext(config)) {
        	
        	//Read sentences
            JavaRDD<String> lineItems = null;
        	for (int i = 2; i < args.length; i++) {
        		if (lineItems == null) {
        			lineItems = context.textFile(args[i]);
        		}
        		else {
        			JavaRDD<String> localLineItems = context
                            .textFile(args[i]);
        			lineItems = lineItems.union(localLineItems);
        		}
        	}

            assert lineItems != null;
            
            JavaRDD<String> splittedSentences = lineItems
					.flatMap(new FlatMapFunction<String, String>() {

						public Iterable<String> call(String t) throws Exception {
							LineItem li = new LineItem(t);
							String lineWithoutSpaces = li.TEXT.replace("\\n", " ");
							Reader reader = new StringReader(lineWithoutSpaces);
							DocumentPreprocessor dp = new DocumentPreprocessor(reader);
							List<String> sentenceList = new ArrayList<String>();
							for (List<HasWord> sentence : dp) {
								String sentenceString = Sentence.listToString(sentence);
								sentenceList.add(sentenceString.toString());
							}
							return sentenceList;
						}
					});

			// tag articles
			JavaRDD<String> taggedSentences = splittedSentences
					.map(new Function<String, String>() {
						public String call(String sentence) {
							if (classifier == null) {
								try {
									classifier = CRFClassifier.getClassifier(classifierPath);
								} catch (IOException e) {
									e.printStackTrace();
								} catch (ClassNotFoundException e) {
									e.printStackTrace();
								}
							}
							String tagged = classifier.classifyToString(sentence, "inlineXML", true);
							return tagged;
						}
					});

			taggedSentences.saveAsTextFile(outputDirectory+"/tagged");
            
            long endTime = Time.now();
            System.out.println("Finished!");
            System.out.println("It took: " + ((endTime - startTime)/1000.0f) + " seconds.");
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
	
	static class SeedTuple implements Serializable {
        String ORGANIZATION;
        String LOCATION;

        SeedTuple(String line) {
            String[] values = line.split("\t");
            ORGANIZATION = values[0];
            LOCATION = values[1];
        }
    }
}
