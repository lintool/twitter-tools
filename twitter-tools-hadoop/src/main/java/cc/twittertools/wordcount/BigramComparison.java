package cc.twittertools.wordcount;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.google.common.collect.BiMap;

import cc.twittertools.compression.CompressionEnsemble;
import cc.twittertools.encoding.HuffmanEncoding;
import cc.twittertools.encoding.HuffmanTree;
import cc.twittertools.encoding.HuffmanTree.Unit;
import cc.twittertools.encoding.VariableByteEncoding;
import cc.twittertools.hbase.WordCountDAO;
import cc.twittertools.hbase.WordCountDAO.WordCount;
import cc.twittertools.wordcount.MemoryCounts;

public class BigramComparison {
	
	public static MemoryCounts M;
	public static BiMap<Unit, String> bigramHuffmanTree;
	public static BiMap<Unit, String> unigramHuffmanTree;
	public static byte[] selector = {(byte)0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01};
	
	public static byte[] getBitVector(String word, int dayDiff) {
		if (!M.termIdTable.contains(word, dayDiff)) {
			byte[] bitVector = new byte[WordCountDAO.NUM_INTERVALS/8];
			for (int i = 0; i < bitVector.length; i += 8) {
				bitVector[i/8] = 0;
			}
			return bitVector;
		}
		int termId = M.termIdTable.get(word, dayDiff);
		int offset = M.offset.get(termId);
		int length = M.length.get(termId);
		byte[] compressData = M.data.subList(offset, offset+length).toByteArray();
		int[] decompressData = VariableByteEncoding.decode(compressData);
		int[] origData = HuffmanEncoding.decode(decompressData, unigramHuffmanTree);
		byte[] bitVector = new byte[origData.length/8];
		for (int i = 0; i < origData.length; i++) {
			bitVector[i/8] <<= 1;
			bitVector[i/8] |= (origData[i] != 0) ? 1 : 0;
		}
		return bitVector;
	}
	
	public static byte[] intersect(byte[] bitVector1, byte[] bitVector2) {
		byte[] intersectBitVector = new byte[bitVector1.length];
		for (int i = 0; i < bitVector1.length; i++) {
			intersectBitVector[i] = (byte) (bitVector1[i] & bitVector2[i]);
		}
		return intersectBitVector;
	}
	
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
					String[] words = prevBigram.split("\\s+");
					int dayDiff = LoadUnigramCount.computeDayDiff(prevDay);
					bitVector1 = getBitVector(words[0], dayDiff);
					bitVector2 = getBitVector(words[1], dayDiff);
					intersectBitVector = intersect(bitVector1, bitVector2);
					IntArrayList bigramCounts = new IntArrayList();
					for (int i = 0; i < w.count.length; i++) {
						if ((intersectBitVector[i/8] & selector[i%8]) != 0) {
							bigramCounts.add(w.count[i]);
						}
					}
					if (bigramCounts.size() > 0) {
						CompressionEnsemble.compression(bigramCounts.toIntArray());
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
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		String bigramCountPath = args[0];
		String bigramHuffmanTreePath = args[1];
		String unigramCountPath = args[2];
		String unigramHuffmanTreePath = args[3];
		M = LoadUnigramCount.load(unigramCountPath);
		unigramHuffmanTree = HuffmanEncoding.loadHuffmanTree(unigramHuffmanTreePath);
		bigramHuffmanTree = HuffmanEncoding.loadHuffmanTree(bigramHuffmanTreePath);
		CompressionEnsemble.init(bigramHuffmanTree);
		LoadBigramCountMap(bigramCountPath);
		CompressionEnsemble.printResults();
	}
}
