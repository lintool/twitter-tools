package cc.twittertools.entropy;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cc.twittertools.encoding.HuffmanTree;
import cc.twittertools.encoding.HuffmanTree.Unit;

public class Entropy {
	
	public static HuffmanTree tree = new HuffmanTree();
	
	public static double computeEntropy(String fileAddr) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(fileAddr));
		HashMap<Unit, Integer> dict = new HashMap<Unit, Integer>();
		int totalCnt = 0;
		String line;
		while( (line=br.readLine()) != null){
			String[] groups = line.split(":");
			String str = groups[0];
			int freq = Integer.parseInt(groups[1]);
			totalCnt += freq;
			dict.put(tree.new Unit(str), freq);
		}
		
		double entropy = 0;
		for(Map.Entry<Unit, Integer> e: dict.entrySet()){
			double prob = e.getValue() * 1.0 / totalCnt;
			entropy += - e.getValue() * Math.log(prob) / Math.log(2);
		}
		return entropy;
	}
	
	public static void main(String[] args) throws IOException{
		String dictFileAddr = args[0];
		double entropy = Entropy.computeEntropy(dictFileAddr);
		System.out.println(entropy/(1024*8));
	}
}
