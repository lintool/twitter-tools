package cc.twittertools.wordcount;

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

import cc.twittertools.compression.CompressionEnsemble;
import cc.twittertools.compression.HuffmanCompression;
import cc.twittertools.compression.RawCountCompression;
import cc.twittertools.compression.WaveletCompression;
import cc.twittertools.encoding.HuffmanEncoding;
import cc.twittertools.encoding.HuffmanTree;
import cc.twittertools.encoding.Simple16Encoding;
import cc.twittertools.encoding.VariableByteEncoding;
import cc.twittertools.encoding.HuffmanTree.Unit;
import cc.twittertools.hbase.WordCountDAO;
import cc.twittertools.hbase.WordCountDAO.WordCount;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class UnigramComparison {
			
	static int recordCnt = 0;
	static long length;
	static BiMap<Unit, String> huffmanTree;
	static int Threshold = 1;
	
	public static void LoadWordCountMapFromFile(String filePath) throws IOException {
		System.out.println("Processing "+filePath);
		BufferedReader bf = new BufferedReader(new FileReader(filePath));
		String line;
		String prevDay = "", prevWord="";
		
		WordCountDAO.WordCount w = null;
		while((line=bf.readLine())!=null){
			String[] groups = line.split("\\t");
			String word = groups[0]; 
			String day = groups[1];
			int interval = Integer.parseInt(groups[2]);
			int count = Integer.parseInt(groups[3]);
			if((!prevDay.equals(day) || !prevWord.equals(word)) && !word.equals("")){
			//if(!prevWord.equals(word) && !word.equals("")){
				if(w != null && w.getSum() > Threshold){
					CompressionEnsemble.compression(w.count);
					if(++recordCnt % 50000 == 0){
						System.out.println("Load " + recordCnt + " records");
					}
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
		String huffmanTreePath = args[1];
		huffmanTree = HuffmanEncoding.loadHuffmanTree(huffmanTreePath);
		CompressionEnsemble.init(huffmanTree);
		LoadWordCountMap(wordCountPath);
		CompressionEnsemble.printResults();
	}

}
