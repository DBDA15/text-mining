package de.hpi.fgis.dbda.textmining.maintask;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.process.DocumentPreprocessor;

public class Tagging
{

	private static String classifierPath = "ner-tagger/classifiers/english.all.3class.distsim.crf.ser.gz";

	private static transient AbstractSequenceClassifier<CoreLabel> classifier = null;

	public static void main( String[] args )
	{

		// initialize spark environment
		SparkConf config = new SparkConf().setAppName(App.class.getName());
		config.set("spark.hadoop.validateOutputSpecs", "false");

		try(JavaSparkContext context = new JavaSparkContext(config)) {
			JavaRDD<String> lineItems = context
					.textFile("data/nyt.2007.tsv");


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

			taggedSentences.saveAsTextFile("results/tagged");

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