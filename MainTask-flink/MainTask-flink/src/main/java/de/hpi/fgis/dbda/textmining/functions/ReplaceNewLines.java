package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.MapFunction;

import de.hpi.fgis.dbda.textmining.MainTask_flink.LineItem;

public class ReplaceNewLines implements MapFunction<String, String> {

	@Override
	public String map(String arg0) throws Exception {
		LineItem li = new LineItem(arg0);
		String text = li.TEXT;
		return text.replace("\\n", " ");
	}

}
