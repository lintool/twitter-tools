package cc.twittertools.wordcount;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.gson.stream.JsonWriter;

import cc.twittertools.encoding.HuffmanTree.Unit;
import cc.twittertools.hbase.WordCountDAO;

public class WordTrendVis {
	
	public static Map<String, List<Integer>> wordToQuery = new HashMap<String, List<Integer>>();
	public static Map<Integer, Query> queryMap = new HashMap<Integer, Query>();
	public static WordTrendVis trend = new WordTrendVis();
	public static boolean isBigram = false;
	
	public class Query{
		static final int capacity = 18;
		int queryId;
		String query;
		String time;
		Map<String, IntArrayList> ngram;
		
		public Query(int queryId, String query, String time){
			this.queryId = queryId;
			this.query = query;
			this.time = time;
			this.ngram = new HashMap<String, IntArrayList>();
			String[] words = query.split("[^a-z0-9\\-]+");
			String[] bigrams = new String[words.length - 1];
			for (int i = 1; i < words.length; i++) {
				bigrams[i - 1] = words[i-1] + " " + words[i];
			}
			String[] tokens = isBigram ? bigrams : words;
			for (int i = 0; i < tokens.length; i++) {
				IntArrayList freq = new IntArrayList();
				for (int j = 0; j < capacity; j++) {
					freq.add(0);
				}
				ngram.put(tokens[i], freq);
			}
		}
		
		public void setNgramFreq(String token, int index, int freq) {
			IntArrayList list = ngram.get(token);
			list.set(index, freq);
		}
	}
	
	public static int computeDayDiff(String queryTime, String wordTime) {
		Date queryDate = null, wordDate = null;
		try {
			queryDate = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy").parse(queryTime);
			wordDate = new SimpleDateFormat("yyyy MMM dd").parse(wordTime);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long diff = queryDate.getTime() - wordDate.getTime();
		return (int) TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
	}
	
	public static void loadQuery(String queryFile) throws IOException {
		BufferedReader bf = new BufferedReader(new FileReader(queryFile));
		String line, queryStr = null, queryTime = null;
		int queryId = 0;
		while((line = bf.readLine()) != null) {
			if (line.startsWith("<num>")) {
				int beginIndex = line.indexOf("MB");
				int endIndex = line.indexOf(" </num>");
				queryId = Integer.parseInt(line.substring(beginIndex+2, endIndex));
			}
			if (line.startsWith("<title>")) {
				int beginIndex = line.indexOf("<title> ");
				int endIndex = line.indexOf(" </title>");
				queryStr = line.substring(beginIndex+8, endIndex).toLowerCase();
				String[] words = queryStr.split("[^a-z0-9\\-]+");
				String[] bigrams = new String[words.length - 1];
				for (int i = 1; i < words.length; i++) {
					bigrams[i - 1] = words[i-1] + " " + words[i];
				}
				String[] tokens = isBigram ? bigrams : words;
				for (int i = 0; i < tokens.length; i++) {
					List<Integer> queryList = wordToQuery.get(tokens[i]);
					if (queryList == null) {
						queryList = new ArrayList<Integer>();
					}
					queryList.add(queryId);
					wordToQuery.put(tokens[i], queryList);
				}
			} else if (line.startsWith("<querytime>")) {
				int beginIndex = line.indexOf("<querytime> ");
				int endIndex = line.indexOf(" </querytime>");
				queryTime = line.substring(beginIndex+12, endIndex);
				Query query = trend.new Query(queryId, queryStr, queryTime);
				queryMap.put(queryId, query);
			}
		}
	}
	
	public static void LoadWordCountMapFromFile(String filePath) throws IOException {
		System.out.println("Processing "+filePath);
		BufferedReader bf = new BufferedReader(new FileReader(filePath));
		String line;
		String prevDay = "", prevWord="";
		long start, end;
		
		WordCountDAO.WordCount w = null;
		while((line=bf.readLine())!=null){
			String[] groups = line.split("\\t");
			String word = groups[0]; // each day is viewed as a column in underlying HBase
			String day = groups[1];
			int interval = Integer.parseInt(groups[2]);
			int count = Integer.parseInt(groups[3]);
			if (wordToQuery.containsKey(word)){
				if((!prevDay.equals(day) || !prevWord.equals(word))){
					if (w != null) {
						for (int queryId : wordToQuery.get(prevWord)) {
							Query query = queryMap.get(queryId);
							int dayDiff = computeDayDiff(query.time, w.column_id);
							int freq = w.getSum();
							if (dayDiff >= 0 && dayDiff < Query.capacity) {
								query.setNgramFreq(prevWord, dayDiff, freq);
							}
							queryMap.put(queryId, query);
						}
					}
					w = new WordCountDAO.WordCount(word, day);
				}
				if(w != null && interval >= 0){
					w.setCount(interval, count);
				}
				prevDay = day;
				prevWord = word;
			} else {
				continue;
			}
		}
		for (int queryId : wordToQuery.get(prevWord)) {
			Query query = queryMap.get(queryId);
			int dayDiff = computeDayDiff(query.time, w.column_id);
			int freq = w.getSum();
			if (dayDiff >= 0 && dayDiff < Query.capacity) {
				query.setNgramFreq(prevWord, dayDiff, freq);
			}
			queryMap.put(queryId, query);
		}
	}
	
	public static void LoadWordCountMap(String path) throws IOException{
		File folder = new File(path);
		if(folder.isDirectory()){
			for (File file : folder.listFiles()) {
				if(!file.getName().startsWith("part"))
					continue;
				String filePath = path+file.getName();
				LoadWordCountMapFromFile(filePath);
			}
		}
	}
	
	public static void saveQueryData (String fileAddr) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(fileAddr));
		JsonWriter jsonWriter = new JsonWriter(bw);
		jsonWriter.setIndent("\t");
		jsonWriter.beginObject();
		for (int queryId = 1; queryId < queryMap.keySet().size(); queryId++) {
			Query query = queryMap.get(queryId);
			jsonWriter.name(String.valueOf(query.queryId)).beginObject();
			jsonWriter.name("query").value(query.query);
			jsonWriter.name("words").beginObject();
			for(String word : query.ngram.keySet()) {
				jsonWriter.name(word).beginArray();
				IntArrayList trends = query.ngram.get(word);
				for (int index = 0; index < Query.capacity; index++) {
					jsonWriter.value(trends.get(index));
				}
				jsonWriter.endArray();
			}
			jsonWriter.endObject();
			jsonWriter.endObject();
		}
		jsonWriter.endObject();
		jsonWriter.flush();
		jsonWriter.close();
		bw.close();
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String filePath = args[0];
		String queryFilePath = args[1];
		String outputPath = args[2];
		if (args.length > 3) {
			isBigram = Boolean.parseBoolean(args[3]);
		}
		loadQuery(queryFilePath);
		LoadWordCountMap(filePath);
		saveQueryData(outputPath);
	}
}
