package edu.illinois.lis.searchsource;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;



import cc.twittertools.search.api.TrecSearchThriftClient;
import cc.twittertools.thrift.gen.TResult;
import edu.illinois.lis.document.FeatureVector;



public class IndexWrapperMicroblogApi {
	// API-specific variables
	private String hostname;
	private int port;
	private String groupId;
	private String authToken;

	private Map<String,String> seenDocs;	// we store the text of any docs we've harvested.  e.g. for FB.

	private TrecSearchThriftClient client;

	
	public IndexWrapperMicroblogApi(String hostname, int port, String groupId, String authToken) {
		this.hostname  = hostname;
		this.port      = port;
		this.groupId   = groupId;
		this.authToken = authToken;
		
		seenDocs = new HashMap<String,String>();
		
		try {
			client = new TrecSearchThriftClient(hostname, port, groupId, authToken);
		} catch (Exception e) {

		}
	}

	public double docCount() {
		return 0;
	}


	public double docFreq(String arg0) {
		return 0;
	}
	
	public double termFreq(String arg0) {
		return 0;
	}

	public double termTokenCount() {
		return 0;
	}

	public double termTypeCount() {
		return 0;
	}

	public Object getActualIndex() {
		return null;
	}
	
	public FeatureVector getDocVector(String docId) {
		if(seenDocs.containsKey(docId))
			return new FeatureVector(seenDocs.get(docId), null);

		// we should also be able to ping the API to get docs we haven't already seen
		return null;
	}

	public List<TResult> runQuery(String query, long upperBoundTime, int count) {
		List<TResult> results = null;
		try {
			results = client.search(query,upperBoundTime, count);
			
			// store our text for future reference
			Iterator<TResult> resultIterator = results.iterator();
			while(resultIterator.hasNext()) {
				TResult result = resultIterator.next();
				seenDocs.put(Long.toString(result.getId()), result.getText());
			}
		} catch (Exception e) {

		}
		return results;
	}


	




}
