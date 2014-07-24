package edu.gslis.ttg.searchers;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cc.twittertools.search.api.TrecSearchThriftClient;
import cc.twittertools.thrift.gen.TResult;
import edu.gslis.queries.GQuery;
import edu.gslis.textrepresentation.FeatureVector;

public class SimpleSearcher {
	
	private TrecSearchThriftClient client;
	private int maxResults;
	
	public SimpleSearcher(TrecSearchThriftClient client, int maxResults) {
		this.client = client;
		this.maxResults = maxResults;
	}
	
	
	public Map<Long, TResult> search(GQuery query) {
		// clean up query
		String queryText = query.getText();
		queryText = queryText.replaceAll("[,'\\.\\?]", " ");
		queryText = queryText.replaceAll("  ", " ").trim();
		
		// need to lowercase the query vector
		FeatureVector temp = new FeatureVector(null);
		Iterator<String> qTerms = query.getFeatureVector().iterator();
		while(qTerms.hasNext()) {
			String term = qTerms.next();
			temp.addTerm(term.toLowerCase(), query.getFeatureVector().getFeatureWeight(term));
		}
		temp.normalize();;
		query.setFeatureVector(temp);
		
		System.err.println(query.getTitle()+": "+queryText);
		
		// perform search
		List<TResult> results = null;
		try {
			results = client.search(queryText, Long.parseLong(query.getMetadata("querytweettime")), maxResults);
		} catch (Exception e) {
			System.err.println("Error searching.");
			System.exit(-1);
		}
		
		// set cutoff score heuristically
		double topScore = results.get(0).getRsv();
		double cutOffScore = topScore / 2;
		
		// record hits, removing duplicates
		int i = 1;
		Map<Long, TResult> seenMap = new HashMap<Long, TResult>(); 
		Iterator<TResult> hitIterator = results.iterator();
		while(hitIterator.hasNext()) {
			TResult hit = hitIterator.next();
			if (hit.getRsv() < cutOffScore) {
				break;
			}
			
			long docId = hit.id;
			if (seenMap.containsKey(docId))
				continue;
			seenMap.put(docId, hit);

			if(i++ >= maxResults)
				break;
		}
		
		return seenMap;
	}

}
