package de.hpi.fgis.dbda.textmining.functions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class ExtractOrganizationSentenceTuples implements FlatMapFunction<String, Tuple2<String, String>> {
	
	private Pattern NERTagPattern = Pattern.compile("<ORGANIZATION>(.+?)</ORGANIZATION>");

	@Override
	public void flatMap(String sentence, Collector<Tuple2<String, String>> arg1) throws Exception {
		Matcher NERMatcher = NERTagPattern.matcher(sentence);
		while (NERMatcher.find()) {
			arg1.collect(new Tuple2(NERMatcher.group(1), sentence));
        }
		
	}

}
