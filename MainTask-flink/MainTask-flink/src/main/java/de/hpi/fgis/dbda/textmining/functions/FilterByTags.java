package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.FilterFunction;

public class FilterByTags implements FilterFunction<String> {
	
	private String tag1;
	private String tag2;

	public FilterByTags(String tag1, String tag2) {
		super();
		this.tag1 = tag1;
		this.tag2 = tag2;
	}

	@Override
	public boolean filter(String arg0) throws Exception {
		return arg0.contains(tag1) && arg0.contains(tag2);
	}

}
