package edu.illinois.lis.utils;

import java.util.Iterator;
import java.util.List;

public class ListUtils {
	
	public static double[] listToArray(List<Double> x) {
		double[] a = new double[x.size()];
		Iterator<Double> it = x.iterator();
		int i=0;
		while(it.hasNext()) {
			a[i++] = it.next();
		}
		return a;
	}
}
