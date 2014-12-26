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

import org.apache.tools.ant.types.resources.Tokens;

import com.google.common.collect.BiMap;
import com.google.gson.stream.JsonWriter;

import cc.twittertools.encoding.HuffmanEncoding;
import cc.twittertools.encoding.VariableByteEncoding;
import cc.twittertools.encoding.HuffmanTree.Unit;
import cc.twittertools.hbase.WordCountDAO;

public class WordTrendVisFromCounts {
	
	public static Map<String, List<Integer>> wordToQuery = new HashMap<String, List<Integer>>();
	public static Map<Integer, Query> queryMap = new HashMap<Integer, Query>();
	public static WordTrendVisFromCounts trend = new WordTrendVisFromCounts();
	public static BiMap<Unit, String> bigramHuffmanTree;
	public static boolean isBigram = true;
	
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
	
	public static int computeDayDiff(String queryTime, int wordDayDiff) {
		Date queryDate = null, baseDate = null;
		try {
			baseDate = new SimpleDateFormat("yyyy-MM-dd").parse("2011-01-23");
			queryDate = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy").parse(queryTime);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long diff = queryDate.getTime() - baseDate.getTime();
		int queryDayDiff = (int) TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
		return queryDayDiff - wordDayDiff;
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
	
	public static void LoadWordCountMapFromFile(String filePath) 
			throws IOException, ClassNotFoundException {
		MemoryCounts memoryCounts;
		memoryCounts = LoadBigramCount.load(filePath);
		byte[] bitVector1, bitVector2, intersectBitVector;
		
		for (String token : wordToQuery.keySet()) {
			String[] words = token.split(" ");
			for (int tokenDay = 0; tokenDay < Query.capacity; tokenDay++) {
				if (memoryCounts.termIdTable.contains(token, tokenDay)) {
					int termId = memoryCounts.termIdTable.get(token, tokenDay);
					int offset = memoryCounts.offset.get(termId);
					int length = memoryCounts.length.get(termId);
					byte[] compressData = memoryCounts.data.subList(offset, offset+length).toByteArray();
					int[] decompressData = VariableByteEncoding.decode(compressData);
					int[] origData = HuffmanEncoding.decode(decompressData, bigramHuffmanTree);
					/*bitVector1 = BigramComparison.getBitVector(words[0], tokenDay);
					bitVector2 = BigramComparison.getBitVector(words[1], tokenDay);
					intersectBitVector = BigramComparison.intersect(bitVector1, bitVector2);*/
					int sum = 0;
					for (int freq: origData) sum += freq;
					for (int query_id : wordToQuery.get(token)) {
						Query query = queryMap.get(query_id);
						int dayDiff = computeDayDiff(query.time, tokenDay);
						if (dayDiff >= 0 && dayDiff < Query.capacity) {
							query.setNgramFreq(token, dayDiff, sum);
						}
						queryMap.put(query_id, query);
					}
				} 
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
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		String filePath = args[0];
		String queryFilePath = args[1];
		String bigramHuffmanTreePath = args[2];
		String outputPath = args[3];
		/*String unigramCountPath = args[3];
		String unigramHuffmanTreePath = args[4];
		String outputPath = args[5];
		BigramComparison.M = LoadUnigramCount.load(unigramCountPath);
		BigramComparison.unigramHuffmanTree = HuffmanEncoding.loadHuffmanTree(unigramHuffmanTreePath);*/
		bigramHuffmanTree = HuffmanEncoding.loadHuffmanTree(bigramHuffmanTreePath);
		loadQuery(queryFilePath);
		LoadWordCountMapFromFile(filePath);
		saveQueryData(outputPath);
	}
}
