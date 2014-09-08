package cc.twittertools.entropy;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;

public class WordUnit {
	// a WordUnit is uniquely determined by word, day, interval
	private String word;
	private int day;
	private int interval;
	private IntArrayList userList;
	private IntArrayList freqList;
	
	public WordUnit(String word, int day, int interval){
		this.word = word;
		this.day = day;
		this.interval = interval;
		this.userList = new IntArrayList();
		this.freqList = new IntArrayList();
	}
	
	public void addUser(int user_id, int freq){
		this.userList.add(user_id);
		this.freqList.add(freq);
	}
	
	public boolean isPrevWordUnit(WordUnit w){
		return word.equals(w.word) && day == w.day
				&& interval == w.interval + 1;
	}
	
	public double selfEntropy(){
		int freqSum = 0;
		for(int freq: freqList){
			freqSum += freq;
		}
		double entropy = 0;
		for(int freq: freqList){
			double prob = freq*1.0/freqSum;
			entropy += - prob * Math.log(prob) / Math.log(2);
		}
		return userList.size() * entropy;
	}
	
	public double relativeEntropy(WordUnit w){
		int freqSum = 0;
		for(int freq: freqList){
			freqSum += freq;
		}
		for(int freq: w.freqList){
			freqSum += freq;
		}
		double entropy1 = 0, entropy2 = 0;
		for(int freq: freqList){
			double prob = freq*1.0/freqSum;
			if (prob == 0) continue;
			entropy1 += - prob * Math.log(prob) / Math.log(2);
		}
		for(int freq: w.freqList){
			double prob = freq*1.0/freqSum;
			if (prob == 0) continue;
			entropy2 += - prob * Math.log(prob) / Math.log(2);
		}
		return Math.abs(userList.size()*entropy1 - w.userList.size()*entropy2);
	}
	
	public String getWord() {
		return word;
	}
	public int getDay() {
		return day;
	}
	public int getInterval() {
		return interval;
	}
	public IntArrayList getUserList() {
		return userList;
	}
	public IntArrayList getFreqList() {
		return freqList;
	}
}	
