package cc.twittertools.search.retrieval;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Class for representing, reading, and brokering a set of indri queries.
 * 
 * @author Miles Efron
 *
 */
public class Queries  {
	ArrayList<String> queryIDs;			// assigned names (IDs) of queries
	ArrayList<String> queryStrings;		// the actual query strings
	ArrayList<String> firstRels;		// a timestamp or other identifier indicating the earliest rel doc
	ArrayList<String> lastRels;			// a timestamp or other identifier indicating the latest rel doc
	
	List<Query> queries;
	Iterator<Query> iterator;
	
	private int i = 0;
	
	public Queries() {
		
		queryIDs = new ArrayList<String>();
		queryStrings = new ArrayList<String>();
		firstRels = new ArrayList<String>();
		lastRels = new ArrayList<String>();
	}
	

	
	public Query getNextQuery() {
		if(i >= queryIDs.size()) {
			return null;
		}
		Query query = this.getQuery(i);
		i++;
		return query;
		
	}
	
	
	public Query getQuery(int i) {

		Query query = new Query();
		query.setQueryName(queryIDs.get(i));
		query.setQueryString(queryStrings.get(i).trim());
		
		if(lastRels != null && lastRels.size() > i) {
			query.setMetadataField("lastrel", lastRels.get(i));
		}
		return query;
	}
	
	public int numQueries() {
		return queries.size();
	}



	// for adding individually //
	
	public void addQueryID(String id) {
		queryIDs.add(id);
	}
	public void addQueryString(String queryString) {
		queryStrings.add(queryString);
	}
	public void addFirstRel(String epochString) {
		firstRels.add(epochString);
	}
	public void addLastRel(String epochString) {
		lastRels.add(epochString);
	}
	
	public int getNumQueries() {
		return queryIDs.size();
	}

	public String getQueryString(int i) {
		return (String)queryStrings.get(i);
	}

	public String getQueryID(int i) {
		return (String)queryIDs.get(i);
	}	
	public String getFirstRel(int i) {
		return firstRels.get(i);
	}
	public String getLastRel(int i) {
		return lastRels.get(i);
	}


	


}
