package cc.twittertools.wordcount;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import com.google.common.collect.BiMap;

import cc.twittertools.compression.CompressionEnsemble;
import cc.twittertools.encoding.HuffmanEncoding;
import cc.twittertools.encoding.HuffmanTree;
import cc.twittertools.encoding.HuffmanTree.Unit;
import cc.twittertools.hbase.WordCountDAO;

public class BuildBigramHuffmanTree {
	
	public static HashMap<Unit, Integer> dict = new HashMap<Unit, Integer>();
	public static BufferedWriter bw;
	
	public static void LoadBigramCountMap(String path) throws IOException{
		File folder = new File(path);
		if(folder.isDirectory()){
			for (File file : folder.listFiles()) {
				if(!file.getName().startsWith("part"))
					continue;
				String filePath = path+file.getName();
				LoadBigramCountMapFromFile(filePath);
			}
		}
	}
	
	public static void LoadBigramCountMapFromFile(String filePath) throws IOException {
		System.out.println("Processing "+filePath);
		BufferedReader bf = new BufferedReader(new FileReader(filePath));
		String line;
		String prevDay = "", prevBigram = "";
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
					bw.write(prevBigram + "\t" + prevDay +"\t");
					String[] words = prevBigram.split("\\s+");
					int dayDiff = LoadUnigramCount.computeDayDiff(prevDay);
					bitVector1 = BigramComparison.getBitVector(words[0], dayDiff);
					bitVector2 = BigramComparison.getBitVector(words[1], dayDiff);
					intersectBitVector = BigramComparison.intersect(bitVector1, bitVector2);
					IntArrayList bigramCounts = new IntArrayList();
					for (int i = 0; i < w.count.length; i++) { // can exist bug in i%8
						if ((intersectBitVector[i/8] & BigramComparison.selector[i%8]) != 0) {
							bigramCounts.add(w.count[i]);
							bw.write(w.count[i] + " ");
						}
					}
					if (bigramCounts.size() > 0) {
						HuffmanEncoding.GenerateFreqDict(bigramCounts.toIntArray(), dict);
					}
					bw.write("\n");
					if (++record % 50000 == 0) {
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
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		String bigramCountPath = args[0];
		String unigramCountPath = args[1];
		String unigramHuffmanTreePath = args[2];
		bw = new BufferedWriter(new FileWriter("bigram-raw-counts.txt"));
		BigramComparison.M = LoadUnigramCount.load(unigramCountPath);
		BigramComparison.unigramHuffmanTree = HuffmanEncoding.loadHuffmanTree(unigramHuffmanTreePath);
		System.out.println(BigramComparison.M.termIdTable.size()+" "+BigramComparison.M.data.size());
		LoadBigramCountMap(bigramCountPath);
		HuffmanEncoding.saveDict(dict, "bigram");
		HuffmanTree tree = HuffmanTree.buildTree(dict);
		BiMap<Unit, String> huffmanCodes = tree.getCodes();
		HuffmanEncoding.saveHuffmanTree(huffmanCodes, "bigram");
		bw.close();
	}

}
