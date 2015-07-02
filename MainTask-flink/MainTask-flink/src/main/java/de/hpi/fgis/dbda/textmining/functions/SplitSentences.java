package de.hpi.fgis.dbda.textmining.functions;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import de.hpi.fgis.dbda.textmining.MainTask_flink.LineItem;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.process.DocumentPreprocessor;

public class SplitSentences implements FlatMapFunction<String, String> {

	@Override
	public void flatMap(String arg0, Collector<String> arg1) throws Exception {
		
		Reader reader = new StringReader(arg0);
		DocumentPreprocessor dp = new DocumentPreprocessor(reader);
		List<String> sentenceList = new ArrayList<String>();
		for (List<HasWord> sentence : dp) {
			String sentenceString = Sentence.listToString(sentence);
			arg1.collect(sentenceString);
		}

	}

}
