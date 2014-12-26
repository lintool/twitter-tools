package cc.twittertools.wordcount;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import cc.twittertools.encoding.HuffmanEncoding;
import cc.twittertools.encoding.HuffmanTree.Unit;
import cc.twittertools.encoding.VariableByteEncoding;
import cc.twittertools.hbase.WordCountDAO;
import cc.twittertools.hbase.WordCountDAO.WordCount;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class LoadUnigramCount {
	
	private static MemoryCounts M = new MemoryCounts();
	private static BiMap<Unit, String> huffmanTree;
	public static int DAYS = 18;
	public static int termId = 0;
	public static int byteCounter = 0;
	
	public static int computeDayDiff(String day) {
		Date currDate = null, baseDate = null;
		try {
			baseDate = new SimpleDateFormat("yyyy-MM-dd").parse("2011-01-23");
			currDate = new SimpleDateFormat("yyyy MMM dd").parse(day);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long diff = currDate.getTime() - baseDate.getTime();
		return (int) TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
	}
	
	public static void LoadWordCountMapFromFile(String filePath) throws IOException {
		System.out.println("Processing "+filePath);
		BufferedReader bf = new BufferedReader(new FileReader(filePath));
		String line;
		String prevDay = "", prevWord="";
		
		WordCountDAO.WordCount w = null;
		while((line=bf.readLine())!=null){
			String[] groups = line.split("\\t");
			String word = groups[0]; // each day is viewed as a column in underlying HBase
			String day = groups[1];
			int interval = Integer.parseInt(groups[2]);
			int count = Integer.parseInt(groups[3]);
			if((!prevDay.equals(day) || !prevWord.equals(word))){
				if (w != null && w.getSum() > UnigramComparison.Threshold) {
					int[] huffmanEncoding = HuffmanEncoding.encode(w.count, huffmanTree);
					byte[] compressData = VariableByteEncoding.encode(huffmanEncoding);
					M.termIdTable.put(prevWord, computeDayDiff(prevDay), termId);
					M.offset.add(byteCounter);
					M.length.add((short) compressData.length);
					M.data.addAll(new ByteArrayList(compressData));
					byteCounter += compressData.length;
					termId++;
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
	
	public static MemoryCounts LoadWordCountToMemory(String wordCountPath,
			String huffmanTreePath) {
		try {
			huffmanTree = HuffmanEncoding.loadHuffmanTree(huffmanTreePath);
			LoadWordCountMap(wordCountPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return M;
	}
	
	public static void save(String fileAddr) throws IOException {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(fileAddr));
	    out.writeObject(M.termIdTable);
	    out.writeObject(M.data);
	    out.writeObject(M.offset);
	    out.writeObject(M.length);
	    out.close();
	}
	
	public static MemoryCounts load(String fileAddr) throws IOException, ClassNotFoundException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(fileAddr));
		MemoryCounts M = new MemoryCounts();
		M.termIdTable = (Table<String, Integer, Integer>) in.readObject();
		M.data = (ByteArrayList) in.readObject();
		M.offset = (IntArrayList) in.readObject();
		M.length = (ShortArrayList) in.readObject();
		in.close();
		return M;
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String wordCountPath = args[0];
		String huffmanTreePath = args[1];
		String outputPath = args[2];
		huffmanTree = HuffmanEncoding.loadHuffmanTree(huffmanTreePath);
		LoadWordCountMap(wordCountPath);
		save(outputPath);
	}

}
