package umd.twittertools.model;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

import umd.twittertools.data.Tweet;
import umd.twittertools.data.TweetSet;
import umd.twittertools.eval.Evaluation;
import umd.twittertools.model.KDEModel.WeightEnum;

import com.google.common.collect.Table;

/*
 * implementation of Recency Prior Model of CIKM 03 Paper
 * X. Li and W. B. Croft, Time-based Language Models, CIKM 2003
 */
public class RecencyModel extends Model{
	private static double begin = 0, end = 1.0, stepSize = 0.001;
	private static double MAP_LAMBDA, P30_LAMBDA;
	public static double EVAL_MAP = 0, EVAL_P30 = 0;
	
	@Override
	public void computeTMScore(TweetSet tweetSet, double lambda) {
		for (Tweet tweet: tweetSet) {
			double dayDiff = (double) tweet.getTimeDiff() / TIME_INTERVAL;
			double qlScore = tweet.getQlScore();
			double tscore = lambda * Math.pow(Math.E, -lambda * dayDiff);
			double tmScore = (tscore != 0) ? (qlScore + Math.log(tscore)) : qlScore;
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
				if (!qrels.containsRow(qid)) {
					continue;
				}
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
		P30_LAMBDA = MAP_LAMBDA; //= 0.032;
		for (int qid : query2TweetSet.keySet()) {
			if (!qrels.containsRow(qid)) {
				continue;
			}
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
		String options = "recency";
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
