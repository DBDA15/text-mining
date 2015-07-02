package de.hpi.fgis.dbda.textmining.MainTask_flink;

import java.io.Serializable;

public class LineItem implements Serializable {
	public Integer INDEX;
	public String TEXT;

	public LineItem(String line) {
		String[] values = line.split("\t");
		INDEX = Integer.parseInt(values[0]);
		TEXT = values[4];
	}
}