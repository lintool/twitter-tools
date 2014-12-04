package umd.twittertools.model;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import umd.twittertools.data.Tweet;
import umd.twittertools.data.TweetSet;
import umd.twittertools.eval.Evaluation;
import umd.twittertools.model.KDEModel.WeightEnum;

import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.Table;
import com.sun.org.apache.xerces.internal.parsers.IntegratedParserConfiguration;

/* 
 * implementation of Moving Window (WIN) approach of TKDE 12 paper
 * Wisam Dakka, Luis Gravano, and Panagiotis G. Ipeirotis, Answering
 * Queries Time-Sensitive Queries, TKDE 2012.
 */
public class WINModel extends Model{
	public static int winsize = 1;
	private static double begin = 0, end = 1.0, stepSize = 0.005;
	private static double MAP_LAMBDA, P30_LAMBDA;
	public static double EVAL_MAP = 0, EVAL_P30 = 0;
	
	public static <K, V extends Comparable<V>> Map<K, V> sortByValues(final Map<K, V> map) {
		Comparator<K> valueComparator =  new Comparator<K>() {
		    public int compare(K k1, K k2) {
		        int compare = map.get(k2).compareTo(map.get(k1));
		        if (compare == 0) return 1;
		        else return compare;
		    }
		};
		Map<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
		sortedByValues.putAll(map);
		return sortedByValues;
	}
	
	private static TreeMap<Integer, Integer> getDailyCount(TweetSet tweetSet) {
		TreeMap<Integer, Integer> binMap = new TreeMap();
		// raw counts per day
		for (Tweet tweet : tweetSet) {
			int dayDiff = (int) tweet.getTimeDiff() / TIME_INTERVAL;
			if (!binMap.containsKey(dayDiff)) {
				binMap.put(dayDiff, 1);
			} else {
				binMap.put(dayDiff, binMap.get(dayDiff)+1);
			}
		}
		return binMap;
	}
	
	private static TreeMap<Integer, Integer> computeBinCounts(
			TreeMap<Integer, Integer> binMap, TweetSet tweetSet) {
		
		// normalize using moving window with size as $winsize
		TreeMap<Integer, Integer> normBinMap = new TreeMap();
		for(int day: binMap.keySet()) {
			double sum = 0, count = 0;
			for (int incre = -winsize; incre <= winsize; incre++) {
				if (binMap.containsKey(day+incre)) {
					sum += binMap.get(day+incre);
					count++;
				}
			}
			normBinMap.put(day, (int) (sum/count));
		}
		
		Map<Integer, Integer> sortedNormBinMap = sortByValues(normBinMap);
		TreeMap<Integer, Integer> day2binMap = new TreeMap();
		int binId = 0;
		double prevCount = Double.MAX_VALUE;
		for (Map.Entry<Integer, Integer> entry: sortedNormBinMap.entrySet()) { 
			if (prevCount > entry.getValue()) {
				binId++;
			}
			day2binMap.put(entry.getKey(), binId);
			prevCount = entry.getValue();
		}
		
		return day2binMap;
	}
	
	@Override
	public void computeTMScore(TweetSet tweetSet, double lambda) {
		TreeMap<Integer, Integer> dailyCountMap = getDailyCount(tweetSet);
		TreeMap<Integer, Integer> day2binMap = computeBinCounts(dailyCountMap, tweetSet);
		double score = 0, totalscore = 0, totalcount = 0;
		for (Map.Entry<Integer, Integer> entry: day2binMap.entrySet()) {
			totalscore += lambda * Math.pow(Math.E, -lambda * entry.getValue());
		}
		for (Map.Entry<Integer, Integer> entry: dailyCountMap.entrySet()) {
			totalcount += entry.getValue();
		}
		
		for (Tweet tweet : tweetSet) {
			double qlScore = tweet.getQlScore();
			int dayDiff = (int) tweet.getTimeDiff() / TIME_INTERVAL;
			int binId = day2binMap.get(dayDiff);
			int count = dailyCountMap.get(dayDiff);
			score = lambda * Math.pow(Math.E, -lambda * binId);
			double temporalProb = (score / totalscore) * ((double) count / totalcount);
			double tmScore = lambda != 0 ? qlScore + Math.log(temporalProb) : qlScore;
			tweet.setTMScore(tmScore);
		}
	}
	
	@Override
	public void computeTMScore(TweetSet tweetSet, TweetSet oracleSet, 
			WeightEnum woption, double lambda){
		System.out.println("Empty Function, Exited");
		System.exit(0);
	}
	
	@Override
	public void train(Map<Integer, TweetSet> query2TweetSet, Table<Integer, Long, Integer> qrels
			, Map<Integer, Integer> numrels) throws Exception {
		PrintStream out = new PrintStream(System.out, true, "UTF-8");
		out.println("Training begins.");
		int interval =(int) ((end - begin) / stepSize) + 1;
		double[] map = new double[interval];
		double[] p30 = new double[interval];
		
		int iter = 0;
		for (double lambda = begin; lambda <= end; lambda += stepSize) {
			for (int qid : query2TweetSet.keySet()) {
				if (!qrels.containsRow(qid)) continue;
				// compute temporal language model score for each tweet
				computeTMScore(query2TweetSet.get(qid), lambda);
				query2TweetSet.get(qid).sortByTMscore();
				map[iter] += Evaluation.MAP(qid, query2TweetSet.get(qid), qrels, numrels);
				p30[iter] += Evaluation.P_RANK(qid, query2TweetSet.get(qid), qrels, 30);
			}
			
			if (debug) {
				out.println(String.format("%.3f : %.4f (MAP) %.4f (P30)", lambda, map[iter]/numOfquerys, 
					p30[iter]/numOfquerys));
			}
			iter++;
		}
		
		// select the ALPHAs to maximize test statistics (MAP and P30)
		double map_avg = 0, map_max = 0;
		double p30_avg = 0, p30_max = 0;
		for (int i = 0; i < interval; i++) {
			if (i == 0 || i == interval - 1) {
				continue;
			} else {
				map_avg = (map[i-1] + map[i] + map[i+1]) / 3.0;
				p30_avg = (p30[i-1] + p30[i] + p30[i+1]) / 3.0;
			}
			if (map_avg > map_max) {
				map_max = map_avg;
				MAP_LAMBDA = (i-1) * stepSize + begin;
			}
			if (p30_avg > p30_max) {
				p30_max = p30_avg;
				P30_LAMBDA = (i-1) * stepSize + begin;
			}
		}
		
		out.println("Training ends.");
		out.println(String.format("Optimal Lambda: %.3f (MAP) %.3f (P30)", MAP_LAMBDA, P30_LAMBDA));
	}
	
	@Override
	public void test(Map<Integer, TweetSet> query2TweetSet, Table<Integer, Long, Integer> qrels
			, Map<Integer, Integer> numrels) throws Exception {
		double[] map_per_query = new double[numOfquerys];
		double[] p30_per_query = new double[numOfquerys];
		EVAL_MAP = EVAL_P30 = 0;
		
		PrintStream out = new PrintStream(System.out, true, "UTF-8");
		out.println("----------------------------------");
		out.println("Testing.");
		
		int counter = 0;
		P30_LAMBDA = MAP_LAMBDA; //= 0.010;
		for (int qid : query2TweetSet.keySet()) {
			if (!qrels.containsRow(qid)) continue;
			computeTMScore(query2TweetSet.get(qid), MAP_LAMBDA);
			query2TweetSet.get(qid).sortByTMscore();
			map_per_query[counter] = Evaluation.MAP(qid, query2TweetSet.get(qid), qrels, numrels);
			computeTMScore(query2TweetSet.get(qid), P30_LAMBDA);
			query2TweetSet.get(qid).sortByTMscore();
			p30_per_query[counter] = Evaluation.P_RANK(qid, query2TweetSet.get(qid), qrels, 30);
			EVAL_MAP += map_per_query[counter];
			EVAL_P30 += p30_per_query[counter];
			counter++;
		}
		EVAL_MAP /= numOfquerys;
		EVAL_P30 /= numOfquerys;
		out.println(String.format("Results: %.4f (MAP) %.4f (P30)", EVAL_MAP, EVAL_P30));
		if (per_query) {
			writeQueryResults(map_per_query, p30_per_query);
		}
	}
	
	public void writeQueryResults(double[] map_per_query, double[] p30_per_query) throws IOException {
		String options = "win";
		String fileName = outputDir + options + ".perquery.txt";
		BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
		for (int qcnt = 0; qcnt < map_per_query.length; qcnt++) {
			int qid = 2 * qcnt + 1;
			bw.write("map " + qid +" " + map_per_query[qcnt] + "\n");
			bw.write("P_30 " + qid +" " + p30_per_query[qcnt] + "\n");
		}
		bw.close();
	}
	
	@Override
	public double getMAP() {
		return EVAL_MAP;
	}
	
	@Override
	public double getP30() {
		return EVAL_P30;
	}
}
