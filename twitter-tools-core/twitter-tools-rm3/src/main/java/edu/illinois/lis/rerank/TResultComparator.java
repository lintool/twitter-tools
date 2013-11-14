package edu.illinois.lis.rerank;

import java.util.Comparator;

import cc.twittertools.thrift.gen.TResult;


public class TResultComparator implements Comparator<TResult>{
	private boolean decreasing = true;
	
	public TResultComparator(boolean decreasing) {
		this.decreasing = decreasing;
	}
	public int compare(TResult x, TResult y) {
		double xVal = x.getRsv();
		double yVal = y.getRsv();
		
		if(decreasing) {
			return (xVal > yVal  ? -1 : (xVal == yVal ? 0 : 1));
		} else {
			return (xVal < yVal  ? -1 : (xVal == yVal ? 0 : 1));
		}

	}

}
