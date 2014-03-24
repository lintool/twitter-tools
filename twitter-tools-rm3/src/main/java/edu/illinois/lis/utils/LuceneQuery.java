package edu.illinois.lis.utils;

import java.util.Iterator;

import edu.illinois.lis.document.FeatureVector;
import edu.illinois.lis.query.GQuery;

public class LuceneQuery {
	public static String gQueryToLucene(GQuery gQuery, int k) {
		FeatureVector mainVector = new FeatureVector(gQuery.getText(), null);
		mainVector.normalizeToOne();
		FeatureVector fbVector = gQuery.getFeatureVector();
		fbVector.pruneToSize(k);
		fbVector.normalizeToOne();
		FeatureVector finalVector = FeatureVector.interpolate(mainVector, fbVector, 0.5);
		StringBuilder b = new StringBuilder();
		Iterator<String> terms = finalVector.iterator();
		while(terms.hasNext()) {
			String term = terms.next();
			double weight = finalVector.getFeaturetWeight(term);
			b.append(term + "^" + weight + " ");
		}
		return b.toString().trim();
	}
}
