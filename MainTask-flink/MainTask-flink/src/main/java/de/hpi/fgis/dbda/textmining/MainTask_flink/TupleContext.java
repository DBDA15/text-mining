package de.hpi.fgis.dbda.textmining.MainTask_flink;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple5;

public class TupleContext extends Tuple5<HashMapWithSemicolons, String, HashMapWithSemicolons, String, HashMapWithSemicolons> {
	
	private Map leftContext;
	private Map middleContext;
	private Map rightContext;
	private String leftEntity;
	private String rightEntity;

	public TupleContext() {
		super(new HashMapWithSemicolons(), "", new HashMapWithSemicolons(), "", new HashMapWithSemicolons());
	}

	public TupleContext(Map _1, String _2, Map _3, String _4, Map _5) {
		super(new HashMapWithSemicolons(_1), _2, new HashMapWithSemicolons(_3), _4, new HashMapWithSemicolons(_5));
	}
	
	public Map getLeftContext() {
		return leftContext;
	}

	public void setLeftContext(Map leftContext) {
		this.leftContext = leftContext;
	}

	public Map getMiddleContext() {
		return middleContext;
	}

	public void setMiddleContext(Map middleContext) {
		this.middleContext = middleContext;
	}

	public Map getRightContext() {
		return rightContext;
	}

	public void setRightContext(Map rightContext) {
		this.rightContext = rightContext;
	}

	public String getLeftEntity() {
		return leftEntity;
	}

	public void setLeftEntity(String leftEntity) {
		this.leftEntity = leftEntity;
	}

	public String getRightEntity() {
		return rightEntity;
	}

	public void setRightEntity(String rightEntity) {
		this.rightEntity = rightEntity;
	}

}
