package cc.twittertools.wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import com.google.common.collect.BiMap;

import cc.twittertools.encoding.HuffmanEncoding;
import cc.twittertools.encoding.HuffmanTree;
import cc.twittertools.encoding.HuffmanTree.Unit;
import cc.twittertools.hbase.WordCountDAO;
import cc.twittertools.hbase.WordCountDAO.WordCount;

public class BuildUnigramHuffmanTree {
	
	static HashMap<Unit, Integer> dict = new HashMap<Unit, Integer>();
	
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
			if((!prevDay.equals(day) || !prevWord.equals(word))){
				if (w != null && w.getSum() > UnigramComparison.Threshold) {
					HuffmanEncoding.GenerateFreqDict(w.count, dict);
				}
				w = new WordCountDAO.WordCount(word);
			}
			if(w != null && interval >= 0){
				w.setCount(interval, count);
			}
			prevDay = day;
			prevWord = word;
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
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String wordCountPath = args[0];
		LoadWordCountMap(wordCountPath);
		HuffmanEncoding.saveDict(dict, "unigram");
		HuffmanTree tree = HuffmanTree.buildTree(dict);
		BiMap<Unit, String> huffmanCodes = tree.getCodes();
		HuffmanEncoding.saveHuffmanTree(huffmanCodes, "unigram");
	}
}
