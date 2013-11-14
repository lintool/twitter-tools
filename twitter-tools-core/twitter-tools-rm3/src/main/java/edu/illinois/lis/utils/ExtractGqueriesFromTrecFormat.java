package edu.illinois.lis.utils;




import java.io.File;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import edu.illinois.lis.query.TrecTemporalTopicSet;


/**
 * creates a simple set of gQueries from the official TREC MB topic file
 * 
 * @author Miles Efron
 *
 */
public class ExtractGqueriesFromTrecFormat {

	private JsonObject outputObjects = null;
	private String pathToTrecTopics;
	
	public ExtractGqueriesFromTrecFormat(String pathToTrecTopics) {
		this.pathToTrecTopics = pathToTrecTopics;
		outputObjects = new JsonObject();
	}

	public void harvest() {	
		TrecTemporalTopicSet topicsFile = null;
		try {
			topicsFile = TrecTemporalTopicSet.fromFile(new File(pathToTrecTopics));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		JsonArray outputJsonArray = new JsonArray();
		for(edu.illinois.lis.query.TrecTemporalTopic query : topicsFile) {
			

			JsonObject outputQueryObject = new JsonObject();
			outputQueryObject.addProperty("title", query.getId());
			outputQueryObject.addProperty("text", query.getQuery());
			outputQueryObject.addProperty("epoch", Double.toString(query.getEpoch()));
			outputQueryObject.addProperty("querytweettime", Long.toString(query.getQueryTweetTime()));
			
			String text = query.getQuery();
			String[] toks = text.split(" ");
			
			JsonArray modelArray = new JsonArray();
			for(String tok : toks) {
				JsonObject tupleObject = new JsonObject();
				tupleObject.addProperty("weight", 1.0);
				tupleObject.addProperty("feature", tok);
				modelArray.add(tupleObject);
			}
			outputQueryObject.add("model", modelArray);


			outputJsonArray.add(outputQueryObject);
		}	
		outputObjects.add("queries", outputJsonArray);
	}


	public String toString() {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String json = gson.toJson(outputObjects);
		return json;
	}




	public static void main(String[] args) throws Exception {
		String trecQueryPath = args[0];

		ExtractGqueriesFromTrecFormat harvester = new ExtractGqueriesFromTrecFormat(trecQueryPath);
		harvester.harvest();

		System.out.println(harvester);
	}



}
