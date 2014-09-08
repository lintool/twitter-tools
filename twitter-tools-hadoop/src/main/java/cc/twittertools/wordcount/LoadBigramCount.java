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
import java.util.concurrent.TimeUnit;

import cc.twittertools.compression.CompressionEnsemble;
import cc.twittertools.encoding.HuffmanEncoding;
import cc.twittertools.encoding.VariableByteEncoding;
import cc.twittertools.encoding.HuffmanTree.Unit;
import cc.twittertools.hbase.WordCountDAO;

import com.google.common.collect.BiMap;
import com.google.common.collect.Table;

public class LoadBigramCount {
	private static MemoryCounts bigramM = new MemoryCounts();
	private static BiMap<Unit, String> bigramHuffmanTree;
	private static int termId = 0;
	private static int byteCounter = 0;
	
	public static void LoadWordCountMapFromFile(String filePath) throws IOException {
		System.out.println("Processing "+filePath);
		BufferedReader bf = new BufferedReader(new FileReader(filePath));
		String line;
		String prevDay = "", prevBigram="";
		byte[] bitVector1, bitVector2, intersectBitVector;
		
		WordCountDAO.WordCount w = null;
		int record = 0;
		while((line=bf.readLine())!=null){
			String[] groups = line.split("\\t");
			String bigram = groups[0]; // each day is viewed as a column in underlying HBase
			String day = groups[1];
			int interval = Integer.parseInt(groups[2]);
			int count = Integer.parseInt(groups[3]);
			if((!prevDay.equals(day) || !prevBigram.equals(bigram))){
				if (w != null) {
					String[] words = prevBigram.split("\\s+");
					int dayDiff = LoadUnigramCount.computeDayDiff(prevDay);
					bitVector1 = BigramComparison.getBitVector(words[0], dayDiff);
					bitVector2 = BigramComparison.getBitVector(words[1], dayDiff);
					intersectBitVector = BigramComparison.intersect(bitVector1, bitVector2);
					IntArrayList bigramCounts = new IntArrayList();
					for (int i = 0; i < w.count.length; i++) {
						if ((intersectBitVector[i/8] & BigramComparison.selector[i%8]) != 0) {
							bigramCounts.add(w.count[i]);
						}
					}
					if (bigramCounts.size() > 0) {
						int[] huffmanEncoding = HuffmanEncoding.encode(bigramCounts.toIntArray(), bigramHuffmanTree);
						byte[] compressData = VariableByteEncoding.encode(huffmanEncoding);
						bigramM.termIdTable.put(prevBigram, dayDiff, termId);
						bigramM.offset.add(byteCounter);
						bigramM.length.add((short) compressData.length);
						bigramM.data.addAll(new ByteArrayList(compressData));
						byteCounter += compressData.length;
						termId++;
					}
					if (record++ % 50000 == 0) {
						System.out.println("Load " + record + " records");
					}
				}
				w = new WordCountDAO.WordCount(bigram);
			}
			if(w != null && interval >= 0){
				w.setCount(interval, count);
			}
			prevDay = day;
			prevBigram = bigram;
		}
		System.out.println("Number of Terms: " + termId);
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
	
	public static void save(String fileAddr) throws IOException {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(fileAddr));
	    out.writeObject(bigramM.termIdTable);
	    out.writeObject(bigramM.data);
	    out.writeObject(bigramM.offset);
	    out.writeObject(bigramM.length);
	    out.close();
	}
	
	public static MemoryCounts load(String fileAddr) throws IOException, ClassNotFoundException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(fileAddr));
		MemoryCounts bigramM = new MemoryCounts();
		bigramM.termIdTable = (Table<String, Integer, Integer>) in.readObject();
		bigramM.data = (ByteArrayList) in.readObject();
		bigramM.offset = (IntArrayList) in.readObject();
		bigramM.length = (ShortArrayList) in.readObject();
		in.close();
		return bigramM;
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		String bigramCountPath = args[0];
		String bigramHuffmanTreePath = args[1];
		String unigramCountPath = args[2];
		String unigramHuffmanTreePath = args[3];
		String outputPath = args[4];
		BigramComparison.M = LoadUnigramCount.load(unigramCountPath);
		BigramComparison.unigramHuffmanTree = HuffmanEncoding.loadHuffmanTree(unigramHuffmanTreePath);
		bigramHuffmanTree = HuffmanEncoding.loadHuffmanTree(bigramHuffmanTreePath);
		LoadWordCountMap(bigramCountPath);
		save(outputPath);
	}
}
