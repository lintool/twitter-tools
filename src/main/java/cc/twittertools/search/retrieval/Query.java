package cc.twittertools.search.retrieval;

import java.util.HashMap;
import java.util.Map;

public class Query {
	private String queryString;
	private String queryName;
	private Map<String,String> metadata;
	
	
	public void setMetadataField(String key, String value) {
		if(metadata == null) {
			metadata = new HashMap<String,String>();
		}
		metadata.put(key, value);
	}
	public String getMetadataField(String key) {
		if(metadata==null || !metadata.containsKey(key)) {
			return null;
		}
		return metadata.get(key);
	}
	
	public String getQueryString() {
		return queryString;
	}
	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}
	public String getQueryName() {
		return queryName;
	}
	public void setQueryName(String queryName) {
		this.queryName = queryName;
	}
	
	

}
