package edu.illinois.lis.query;


import java.util.HashMap;
import java.util.Map;

import edu.illinois.lis.document.FeatureVector;


/**
 * a fairly rich representation of a query (or query-like) object.  at a minimum, it will typically contain a 
 * name some text.
 *  
 * @author Miles Efron
 *
 */
public class GQuery {
	private String name;
	private String text;
	private double epoch = -1.0;
	private long querytweettime = -1L;
	private FeatureVector featureVector;
	

	public String getTitle() {
		return name;
	}
	public String getText() {
		return text;
	}
	public void setTitle(String name) {
		this.name = name;
	}
	public void setText(String text) {
		this.text = text;
	}
	public void setEpoch(double epoch) {
		this.epoch = epoch;
	}
	public void setQuerytweettime(long querytweettime) {
		this.querytweettime = querytweettime;
	}
	public double getEpoch() {
		return epoch;
	}
	public long getQuerytweettime() {
		return querytweettime;
	}


	public FeatureVector getFeatureVector() {
		return featureVector;
	}
	public void setFeatureVector(FeatureVector featureVector) {
		this.featureVector = featureVector;
	}
	
}
