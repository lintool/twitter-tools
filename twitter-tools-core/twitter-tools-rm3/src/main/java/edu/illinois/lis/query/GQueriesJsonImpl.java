package edu.illinois.lis.query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;


import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import edu.illinois.lis.document.FeatureVector;


/**
 * reads and holds GQueries stored as a serialized JSON file on disk.
 * 
 * @author Miles Efron
 *
 */
public class GQueriesJsonImpl implements GQueries {
	private static final Logger LOG = Logger.getLogger(GQueriesJsonImpl.class);

	private static final JsonParser JSON_PARSER = new JsonParser();
	private List<GQuery> queryList;
	private Map<String,Integer> nameToIndex;

	public void read(String pathToQueries) {
		JsonObject obj = null;
		try {
			obj = (JsonObject) JSON_PARSER.parse(new BufferedReader(new FileReader(pathToQueries)));
		} catch (Exception e) {
			LOG.fatal("died reading queries from json file", e);
			System.exit(-1);
		}

		
		JsonArray queryObjectArray = obj.getAsJsonArray("queries");
		queryList = new ArrayList<GQuery>(queryObjectArray.size());
		nameToIndex = new HashMap<String,Integer>(queryList.size());
		Iterator<JsonElement> queryObjectIterator = queryObjectArray.iterator();
		int k=0;
		while(queryObjectIterator.hasNext()) {
			JsonObject queryObject = (JsonObject) queryObjectIterator.next();
			String title = queryObject.get("title").getAsString();
			String text  = queryObject.get("text").getAsString();
			double epoch = queryObject.get("epoch").getAsDouble();
			long querytweettime = queryObject.get("querytweettime").getAsLong();
			nameToIndex.put(title, k++);
			FeatureVector featureVector = new FeatureVector(null);
			JsonArray modelObjectArray = queryObject.getAsJsonArray("model");
			Iterator<JsonElement> featureIterator = modelObjectArray.iterator();
			while(featureIterator.hasNext()) {
				JsonObject featureObject = (JsonObject)featureIterator.next();
				double weight  = featureObject.get("weight").getAsDouble();
				String feature = featureObject.get("feature").getAsString();
				featureVector.addTerm(feature, weight);
			}
			
			
			GQuery gQuery = new GQuery();
			gQuery.setTitle(title);
			gQuery.setText(text);
			gQuery.setEpoch(epoch);
			gQuery.setQuerytweettime(querytweettime);
			gQuery.setFeatureVector(featureVector);
			
			queryList.add(gQuery);
			
		}	
	}

	public GQuery getIthQuery(int i) {
		if(queryList == null || i >= queryList.size()) {
			LOG.fatal("died trying to get query number " + i + "  when we have only " + queryList.size() + " queries.");
			System.exit(-1);		
		}
		return queryList.get(i);
	}
	
	public GQuery getNamedQuery(String queryName) {
		if(queryList == null || ! nameToIndex.containsKey(queryName)) {
			LOG.fatal("died trying to get query  " + queryName + ".");
			System.exit(-1);		}
		return queryList.get(nameToIndex.get(queryName));
	}
	

	public Iterator<GQuery> iterator() {
		return queryList.iterator();
	}

	public int numQueries() {
		return queryList.size();
	}
	
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		
		Iterator<GQuery> it = queryList.iterator();
		while(it.hasNext()) {
			b.append(it.next());
		}
		
		return b.toString();
	}

	

}
