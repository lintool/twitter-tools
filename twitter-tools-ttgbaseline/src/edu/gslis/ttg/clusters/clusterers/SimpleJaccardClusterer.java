package edu.gslis.ttg.clusters.clusterers;

import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import cc.twittertools.thrift.gen.TResult;
import edu.gslis.ttg.clusters.Clusters;
import edu.gslis.ttg.jaccard.JaccardStore;

public class SimpleJaccardClusterer {
	
	private List<TResult> results;
	private JaccardStore jaccardScores;
	
	public SimpleJaccardClusterer(List<TResult> results) {
		this.results = results;
		this.jaccardScores = computeJaccardSimilarity();
	}
	
	public Clusters cluster(double threshold) {
		Clusters clusters = new Clusters();
		
		NavigableMap<Double, List<long[]>> thresholdPairs = jaccardScores.getDocsGreaterThanScore(threshold);
		Iterator<Double> pairsIt = thresholdPairs.keySet().iterator();
		while (pairsIt.hasNext()) { // for each pair of documents matching this jaccard score
			List<long[]> docPairs = thresholdPairs.get(pairsIt.next());
			Iterator<long[]> docPairIt = docPairs.iterator();
			while (docPairIt.hasNext()) { // 
				long[] docs = docPairIt.next();
				clusters.mergeMembers(docs[0], docs[1]);
			}
		}
		
		return clusters;
	}
	
	public List<TResult> getResults() {
		return results;
	}

	public void setResults(List<TResult> results) {
		this.results = results;
	}
	
	private JaccardStore computeJaccardSimilarity() {	
		// compute jaccard similarity for each pair of results
		JaccardStore scores = new JaccardStore();
		for (int j = 0; j < results.size(); j++) {
			TResult doc1 = results.get(j);
			for (int k = j + 1; k < results.size(); k++) {
				TResult doc2 = results.get(k);
				
				double jaccardSim = JaccardStore.computeJaccardSimilarity(doc1.getText(), doc2.getText());
				scores.setScore(doc1.getId(), doc2.getId(), jaccardSim);
			}
		}
		
		return scores;
	}

}
