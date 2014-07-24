package edu.gslis.ttg.jaccard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class JaccardStore {
	
	private Map<long[], Double> scores; // <docPair, jaccardScore>
	private TreeMap<Double, List<long[]>> scoreLookup; // <jaccardScore, docPairs>
	
	public JaccardStore() {
		scores = new HashMap<long[], Double>(); 
		scoreLookup = new TreeMap<Double, List<long[]>>();
	}
	
	public double getScore(long doc1, long doc2) {
		return scores.get(ordered(doc1, doc2));
	}
	
	public void setScore(long doc1, long doc2, double score) {
		scores.put(ordered(doc1, doc2), score);
		if (scoreLookup.get(score) == null) {
			scoreLookup.put(score, new ArrayList<long[]>());
		}
		scoreLookup.get(score).add(ordered(doc1, doc2));
	}
	
	public List<long[]> getDocsForScore(double score) {
		return scoreLookup.get(score);
	}
	
	public NavigableMap<Double, List<long[]>> getDocsGreaterThanScore(double score) {
		return scoreLookup.tailMap(score, true);
	}
	
	public int size() {
		return scores.keySet().size();
	}
	
	private long[] ordered(long doc1, long doc2) {
		long[] ordered = new long[2];
		if (doc1 < doc2) {
			ordered[0] = doc1;
			ordered[1] = doc2;
		} else {
			ordered[0] = doc2;
			ordered[1] = doc1;
		}
		return ordered;
	}
	
	public static double computeJaccardSimilarity(Set<String> doc1, Set<String> doc2) {
		Set<String> intersection = new HashSet<String>(doc1);
		Set<String> union = new HashSet<String>(doc1);
		
		intersection.retainAll(doc2);
		union.addAll(doc2);
		
		return intersection.size() / (double) union.size();
	}
	
	public static double computeJaccardSimilarity(String doc1, String doc2) {
		String[] docOneTerms = doc1.toLowerCase().split("[^A-Za-z0-9]");
		List<String> termList = new ArrayList<String>(Arrays.asList(docOneTerms));
		termList.removeAll(Arrays.asList("", null));
		Set<String> docOneBag = new HashSet<String>(termList);
		
		String[] docTwoTerms = doc2.toLowerCase().split("[^A-Za-z0-9]");
		termList = new ArrayList<String>(Arrays.asList(docTwoTerms));
		termList.removeAll(Arrays.asList("", null));
		Set<String> docTwoBag = new HashSet<String>(termList);
		
		return computeJaccardSimilarity(docOneBag, docTwoBag);
	}

}
