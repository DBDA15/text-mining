package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.FilterFunction;

import java.util.List;

public class FilterByTags implements FilterFunction<String> {
	
	private List<String> entities;

	public FilterByTags(List<String> entityTags) {
		super();
		this.entities = entityTags;
	}

	@Override
	public boolean filter(String arg0) throws Exception {
		return arg0.contains(this.entities.get(0)) && arg0.contains(this.entities.get(1));
	}

}
