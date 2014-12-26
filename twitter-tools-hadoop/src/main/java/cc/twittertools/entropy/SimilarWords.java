package cc.twittertools.entropy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import cc.twittertools.entropy.WordEntropy.Entropy;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Table;

public class SimilarWords {
	
	public class Pair {
		String word1;
		String word2;
		double entropy;
		
		public Pair(String w1, String w2, double entropy){
			this.word1 = w1;
			this.word2 = w2;
			this.entropy = entropy;
		}
	}
	
	public class PairComparator implements Comparator<Pair>{
		public int compare(Pair e1, Pair e2){
			if (e1.entropy < e2.entropy) return 1;
			else return -1;
		}
	}
	
	public static double entropy(IntArrayList list){
		int sum = 0;
		for (int element: list) {
			sum += element;
		}
		if (sum == 0) {
			return 0;
		}
		
		if(sum > 1) {
			boolean debug = true;
		}
		
		double entropy = 0;
		for(int element: list) {
			double prob = element * 1.0 / sum;
			if (prob == 0) continue;
			entropy += prob * Math.log(prob) / Math.log(2);
		}
		return sum / (entropy + 1);
	}
	
	public static int intersection(IntSortedSet set1, IntSortedSet set2) {
		int commonUser = 0;
		int pos1 = 0, pos2 = 0;
		
		int[] userList1 = set1.toIntArray();
		int[] userList2 = set2.toIntArray();
		while(pos1 < userList1.length && pos2 < userList2.length) {
			int user1 = userList1[pos1];
			int user2 = userList2[pos2];
			if (user1 == user2) {
				commonUser++;
				pos1++;
				pos2++;
			} else if (user1 < user2) {
				pos1++;
			} else{
				pos2++;
			}
		}
		
		return commonUser;
	}
	
	public static double similarity(Table<Integer, Integer, WordUnit> word1,
			Table<Integer, Integer, WordUnit> word2) {
		IntArrayList overlap = new IntArrayList();
		
		for(int day = 0; day < WordEntropy.NUM_DAYS; day++){
			IntAVLTreeSet set1 = new IntAVLTreeSet();
			IntAVLTreeSet set2 = new IntAVLTreeSet();
			for(int interval = 0; interval < WordEntropy.NUM_INTERVALS; interval++){
				WordUnit unit1 = word1.get(day, interval);
				WordUnit unit2 = word2.get(day, interval);
				if (unit1 != null){
					set1.addAll(unit1.getUserList());
				}
				if (unit2 != null){
					set2.addAll(unit2.getUserList());
				}
			}
			if (set1.size() > 10 && set2.size() > 10){
				boolean debug = true;
			}
			overlap.add(intersection(set1, set2));
		}
		
		return entropy(overlap);
	}
	
	public static HashMap<String, Table<Integer, Integer, WordUnit>> 
		loadHotWords(String fileName) throws IOException{
		HashMap<String, Table<Integer, Integer, WordUnit>> hotWords = new HashMap<String, Table<Integer, Integer, WordUnit>>();
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String word = null, prevWord = null;
		String line;
		while((line = br.readLine()) != null) {
			String[] groups = line.split(" ");
			word = groups[0];
			int day = Integer.parseInt(groups[1]);
			int interval = Integer.parseInt(groups[2]);
			if (prevWord != word) {
				Table<Integer, Integer, WordUnit> wordMap = HashBasedTable.create();
				hotWords.put(word, wordMap);
			}
			Table<Integer, Integer, WordUnit> wordMap = hotWords.get(word);
			WordUnit unit = new WordUnit(word, day, interval);
			for(int i=3; i < groups.length; i++){
				unit.addUser(Integer.parseInt(groups[i]), 1);
			}
			wordMap.put(day, interval, unit);
			hotWords.put(word, wordMap);
		}
		
		return hotWords;
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		SimilarWords S = new SimilarWords();
		PairComparator pairComparator = S.new PairComparator();
		MinMaxPriorityQueue<Pair> heap = MinMaxPriorityQueue
				.orderedBy(pairComparator)
				.maximumSize(10)
				.create();
		
		HashMap<String, Table<Integer, Integer, WordUnit>> hotWords = loadHotWords(args[0]);
		for(String word1 : hotWords.keySet()) {
			for(String word2: hotWords.keySet()) {
				Table<Integer, Integer, WordUnit> wordMap1 = hotWords.get(word1);
				Table<Integer, Integer, WordUnit> wordMap2 = hotWords.get(word2);
				if(word1.compareTo(word2) > 0) {
					double entropy = similarity(wordMap1, wordMap2);
					heap.add(S.new Pair(word1, word2, entropy));
				}
			}
		}
		
		Iterator<Pair> iterator = heap.iterator();
		while(iterator.hasNext()){
			Pair e = iterator.next();
			System.out.println(String.format("%s %s %f", e.word1, e.word2, e.entropy));
		}
	}

}
