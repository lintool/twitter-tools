package cc.twittertools.hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTablePool;

public class LoadWordCount {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		if(args.length!=1){
			System.out.println("invalid argument");
		}
		BufferedReader bf = new BufferedReader(new FileReader(args[0]));
		// each line in wordcount file is like : 1 twitter 100
		HashMap<String,WordCountDAO.WordCount> wordcounts = new HashMap<String,WordCountDAO.WordCount>();
		String line;
		while((line=bf.readLine())!=null){
			String[] groups = line.split("\\s+");
			if(groups.length != 3) 
				continue;
			String interval = groups[0];
			String word = groups[1];
			String count = groups[2];
			if(!wordcounts.containsKey(word)){
				WordCountDAO.WordCount w = new WordCountDAO.WordCount(word);
				wordcounts.put(word, w);
			}
			WordCountDAO.WordCount w = wordcounts.get(word);
			w.setCount(Integer.valueOf(interval), count);
			wordcounts.put(word, w);
		}
		
		HTablePool pool = new HTablePool();
		WordCountDAO DAO = new WordCountDAO(pool);
		for(Map.Entry<String, WordCountDAO.WordCount> e: wordcounts.entrySet()){
			WordCountDAO.WordCount w = e.getValue();
			DAO.addWordCount(w);
		}
		pool.closeTablePool(DAO.TABLE_NAME);
	}

}
