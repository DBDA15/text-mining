package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.MapFunction;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;

public class TagSentences implements MapFunction<String, String> {

	private AbstractSequenceClassifier<CoreLabel> classifier;
	
	private final String classifierPath = "edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz";

	@Override
	public String map(String arg0) throws Exception {
		if (classifier == null) {
			classifier = CRFClassifier.getClassifier(classifierPath);
		}
		String tagged = classifier.classifyToString(arg0, "inlineXML", true);
		return tagged;
	}

}
