package cc.twittertools.encoding;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import cc.twittertools.encoding.HuffmanTree.Unit;
import cc.twittertools.hbase.WordCountDAO;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Table;

public class HuffmanEncoding {
	
	static int interval = 8;
	static HuffmanTree tree = new HuffmanTree();
	
	public static int[] encode(int[] counts, BiMap<Unit, String> huffmanCodes){
		String code = "";
		for(int i = 0; i<counts.length; i += interval){
			Unit u = tree.new Unit(Arrays.copyOfRange(counts, i, Math.min(i+interval, counts.length)));
			code += huffmanCodes.get(u);
		}
		
		//convert string to int array
		int[] codeArray = new int[(int)Math.ceil(code.length()/31.0) + 1];
		codeArray[0] = code.length() % 31;
		for(int i=0; i<code.length(); i += 31){
			String s = code.substring(i, Math.min(i+31, code.length()));
			int num = Integer.parseInt(s, 2);
			codeArray[i/31+1] = num;
		}
		return codeArray;
	}
	
	public static int[] decode(int[] codes, BiMap<Unit, String> huffmanCodes){
		// convert int array to byte string.
		String codeStr = "";
		String zeroStr = "0000000000000000000000000000000"; //31-bit zero string
		int lengthOfLastString = codes[0];
		for(int i=1; i<codes.length; i++){
			if(lengthOfLastString !=0 && i == codes.length - 1){
				String s = Integer.toBinaryString(codes[i]);
				codeStr += zeroStr.substring(0, lengthOfLastString - s.length()) + s;
			}else{
				String s = Integer.toBinaryString(codes[i]);
				codeStr += zeroStr.substring(0, 31-s.length()) + s;
			}
		}
		
		BiMap<String, Unit> huffmanCodesReverse = huffmanCodes.inverse();
		IntArrayList origData = new IntArrayList();
		String unitStr = "";
		for(int i = 0; i < codeStr.length(); i++){
			unitStr += codeStr.charAt(i);
			if(huffmanCodesReverse.containsKey(unitStr)){
				Unit u = huffmanCodesReverse.get(unitStr);
				for(int element: u.data){
					origData.add(element);
				}
				unitStr = "";
			}
		}
		return origData.toIntArray();
	}
	
	public static void saveDict(HashMap<Unit, Integer> dict, String type) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(type+"-dict.txt"));
		for(Map.Entry<Unit, Integer> e: dict.entrySet()){
			bw.write(e.getKey().toString() + ":" + e.getValue() + "\n");
		}
		bw.close();
	}
	
	public static HashMap<Unit, Integer> loadDict(String fileAddr) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(fileAddr));
		HashMap<Unit, Integer> dict = new HashMap<Unit, Integer>();
		String line;
		while( (line=br.readLine()) != null){
			String[] groups = line.split(":");
			dict.put(tree.new Unit(groups[0]), Integer.parseInt(groups[1]));
		}
		return dict;
	}
	
	public static void saveHuffmanTree(BiMap<Unit, String> huffmanCodes, String type) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(type+"-huffmantree.txt"));
		for(Map.Entry<Unit, String> e: huffmanCodes.entrySet()){
			bw.write(e.getKey().toString() + ":" + e.getValue() + "\n");
		}
		bw.close();
	}
	
	public static BiMap<Unit, String> loadHuffmanTree(String fileAddr) throws IOException{
		BiMap<Unit, String> huffmanCodes = HashBiMap.create();
		BufferedReader br = new BufferedReader(new FileReader(fileAddr));
		String line;
		while( (line=br.readLine()) != null){
			String[] groups = line.split(":");
			huffmanCodes.put(tree.new Unit(groups[0]), groups[1]);
		}
		return huffmanCodes;
	}
	
	public static void GenerateFreqDict(
		int[] count, HashMap<Unit, Integer> dict){
		for(int i=0; i<count.length; i += interval){
			Unit unit = tree.new Unit(Arrays.copyOfRange(count, i, Math.min(i+interval, count.length)));
			if(dict.containsKey(unit)){
				dict.put(unit, dict.get(unit)+1);
			}else{
				dict.put(unit, 1);
			}
		}
	}
}
