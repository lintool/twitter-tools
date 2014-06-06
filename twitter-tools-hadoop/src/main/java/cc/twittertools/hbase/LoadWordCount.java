package cc.twittertools.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class LoadWordCount {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		if(args.length!=1){
			System.out.println("invalid argument");
		}
		Table<String, String, WordCountDAO.WordCount> wordCountMap = HashBasedTable.create();
		File folder = new File(args[0]);
		if(folder.isDirectory()){
			for (File file : folder.listFiles()) {
				if(!file.getName().startsWith("part"))
					continue;
				System.out.println("Processing "+args[0]+file.getName());
				BufferedReader bf = new BufferedReader(new FileReader(args[0]+file.getName()));
				// each line in wordcount file is like : 1 twitter 100
				String line;
				while((line=bf.readLine())!=null){
					String[] groups = line.split("\\t");
					if(groups.length != 4) 
						continue;
					String day = groups[0]; // each day is viewed as a column in underlying HBase
					String interval = groups[1];
					String word = groups[2];
					String count = groups[3];
					if(!wordCountMap.contains(word, day)){
						WordCountDAO.WordCount w = new WordCountDAO.WordCount(word, day);
						wordCountMap.put(word, day, w);
					}
					WordCountDAO.WordCount w = wordCountMap.get(word, day);
					w.setCount(Integer.valueOf(interval), Integer.valueOf(count));
					wordCountMap.put(word, day, w);
					
				}
			}
		}
		
		System.out.println("Total "+wordCountMap.size()+" words");
		HTablePool pool = new HTablePool();
		WordCountDAO DAO = new WordCountDAO(pool);
		DAO.CreateTable();
		int count = 0;
		for(WordCountDAO.WordCount w: wordCountMap.values()){
			DAO.addWordCount(w);
			if(++count % 50000==0){
				System.out.println("Loading "+count+" words");
			}
		}
		pool.closeTablePool(DAO.TABLE_NAME);
	}

}
