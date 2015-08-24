package de.hpi.fgis.dbda.textmining.functions;

import java.util.HashMap;
import java.util.Map;

import de.hpi.fgis.dbda.textmining.MainTask_flink.HashMapWithSemicolons;

public class ContextMapper {

	public static Map<String, Double> mapContext(String context) {
		Map<String, Double> outputMap = new HashMapWithSemicolons<String, Double>();
		context = context.replaceAll("\\{", "");
		context = context.replaceAll("\\}", "");
		context = context.replaceAll("\\(", "");
		context = context.replaceAll("\\)", "");
		context = context.replaceAll("(comma)", ",");
		String[] contextSplits = context.split("; ");
		for (String s : contextSplits) {
			String[] keyValueSplit = s.split("=");
			if (keyValueSplit.length > 1) {
				String key = keyValueSplit[0];
				if (keyValueSplit[1] != null && keyValueSplit[1].length() > 0) {
					outputMap.put(key, Double.parseDouble(keyValueSplit[1]));
				}
			}
		}
		return outputMap;
	}
	
}
