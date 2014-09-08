package cc.twittertools.entropy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

import cc.twittertools.preprocessing.GenerateIDMapping;

public class WordEntropy {
	
	static BufferedWriter bw;
	static BufferedWriter topWordsWriter;
	static int threshold;
	static int NUM_DAYS = 17;
	static int NUM_INTERVALS = 24;
	
	public class Entropy{
		String word;
		double entropy;
		
		public Entropy(String word, double entropy){
			this.word = word;
			this.entropy = entropy;
		}
	}
	
	public class EntropyComparator implements Comparator<Entropy>{
		public int compare(Entropy e1, Entropy e2){
			if (e1.entropy < e2.entropy) return 1;
			else return -1;
		}
	}
	
	public static void saveWord(Table<Integer, Integer, WordUnit> wordUnitMap) throws IOException{
		for(Cell<Integer, Integer, WordUnit> cell: wordUnitMap.cellSet()){
			WordUnit unit = cell.getValue();
			topWordsWriter.write(String.format("%s %d %d ", unit.getWord(), cell.getRowKey(),
					cell.getColumnKey()));
			for(int user: unit.getUserList()){
				topWordsWriter.write(String.format("%d ", user));
			}
			topWordsWriter.write("\n");
		}
	}
	
	public static double computeGlobalWordEntropy(Table<Integer, Integer, WordUnit> wordUnitMap){
		int globalCount = 0;
		int[] dayCount = new int[NUM_DAYS];
		for (int rowKey = 0; rowKey < NUM_DAYS; rowKey++) {
			dayCount[rowKey] = 0;
			for (int columnKey = 0; columnKey < NUM_INTERVALS; columnKey++) {
				if (wordUnitMap.contains(rowKey, columnKey)){
					int num_users = wordUnitMap.get(rowKey, columnKey).getUserList().size();
					dayCount[rowKey] += num_users;
					globalCount += num_users;
				}
			}
		}
		
		double globalEntropy = 0;
		for (int rowKey = 0; rowKey < NUM_DAYS; rowKey++) {
			if (dayCount[rowKey] != 0){
				double prob = dayCount[rowKey] / (double) globalCount;
				globalEntropy +=  - prob * Math.log(prob) / Math.log(2);
			}
		}
		return globalEntropy;
	}
	
	public static double computeWordEntropy(Table<Integer, Integer, WordUnit> wordUnitMap)
			throws IOException{
		double localEntropy = 0;
		String word = null;
		int maxEntropyRowKey = 0, maxEntropyColKey = 0;
		for(Cell<Integer, Integer, WordUnit> c : wordUnitMap.cellSet()){
			int rowKey = c.getRowKey();
			int columnKey = c.getColumnKey();
			WordUnit currUnit = c.getValue();
			word = currUnit.getWord();
			WordUnit prevUnit = null;
			if (columnKey != 0) {
				prevUnit = wordUnitMap.get(rowKey, columnKey - 1);
			} else if(columnKey == 0 && rowKey != 0) {
				prevUnit = wordUnitMap.get(rowKey - 1, 23);
			} else {
				continue;
			}
			double relativeEntropy;
			if (prevUnit != null) {
				relativeEntropy = currUnit.relativeEntropy(prevUnit);
			} else {
				relativeEntropy = currUnit.selfEntropy();
			}
			if (localEntropy < relativeEntropy) {
				localEntropy = relativeEntropy;
				maxEntropyRowKey = rowKey;
				maxEntropyColKey = columnKey;
			}
		}
		
		double globalEntropy = computeGlobalWordEntropy(wordUnitMap);
		double entropy = (globalEntropy < 2) ? localEntropy / (globalEntropy+1) : 0; 
		if (entropy > threshold){
			saveWord(wordUnitMap);
			bw.write(word+" "+localEntropy+" "+globalEntropy+" "
					+maxEntropyRowKey+" "+maxEntropyColKey+"\n");
			for(int rowKey = 0; rowKey < NUM_DAYS; rowKey++){
				bw.write(rowKey + ": ");
				for(int colKey = 0; colKey < NUM_INTERVALS; colKey++){
					if(wordUnitMap.contains(rowKey, colKey)){
						bw.write(wordUnitMap.get(rowKey, colKey).getUserList().size()+" ");
					}else{
						bw.write("0 ");
					}
				}
				bw.write("\n");
			}
		}
		return entropy;
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String path = args[0];
		String idMappingFile = args[1];
		int numOfResults = Integer.parseInt(args[2]);
		threshold = Integer.parseInt(args[3]);
		bw = new BufferedWriter(new FileWriter("entropy.txt"));
		topWordsWriter = new BufferedWriter(new FileWriter("top-words.txt"));
		//UriMapping idMap = new UriMapping(idMappingFile);
		HashMap<String, Integer> idMap = GenerateIDMapping.loadIDMapping(idMappingFile);
		System.out.println("Load ID Mapping Successfully!");
		
		Table<Integer, Integer, WordUnit> wordUnitMap = HashBasedTable.create();
		WordEntropy W = new WordEntropy();
		final EntropyComparator comparator = W.new EntropyComparator();
		MinMaxPriorityQueue<Entropy> heap = MinMaxPriorityQueue
				.orderedBy(comparator)
				.maximumSize(numOfResults)
				.create();
										
		WordUnit currUnit;
		String prevWord = "";
		File folder = new File(path);
		if (folder.isDirectory()) {
			for (File file : folder.listFiles()) {
				if (!file.getName().startsWith("part"))
					continue;
				String filePath = path + file.getName();
				System.out.println("Processing "+filePath);
				BufferedReader bf = new BufferedReader(new FileReader(filePath));
				String line;
				while ((line = bf.readLine()) != null) {
					String[] groups = line.split("\\t");
					String word = groups[0];
					int day = Integer.parseInt(groups[1]);
					int interval = Integer.parseInt(groups[2]);
					//int userId = idMap.getID(groups[3]);
					int userId = idMap.get(groups[3]);
					int freq = Integer.parseInt(groups[4]);
					if (!prevWord.equals(word)) {
						double entropy = computeWordEntropy(wordUnitMap);
						heap.add(W.new Entropy(prevWord, entropy));
						wordUnitMap.clear();
						currUnit = new WordUnit(word, day, interval);
						wordUnitMap.put(day, interval, currUnit);
					}
					if (!wordUnitMap.contains(day, interval)) {
						currUnit = new WordUnit(word, day, interval);
						wordUnitMap.put(day, interval, currUnit);
					}
					currUnit = wordUnitMap.get(day, interval);
					currUnit.addUser(userId, freq);
					wordUnitMap.put(day, interval, currUnit);
					prevWord = word;
				}
			}
		}
		
		Iterator<Entropy> iterator = heap.iterator();
		while(iterator.hasNext()){
			Entropy e = iterator.next();
			System.out.println(e.word+":"+e.entropy);
		}
		bw.close();
		topWordsWriter.close();
	}

}
