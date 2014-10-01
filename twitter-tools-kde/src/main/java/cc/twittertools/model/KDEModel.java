package cc.twittertools.model;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.google.common.base.Charsets;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Table;
import com.google.common.io.Files;

import umontreal.iro.lecuyer.probdist.NormalDist;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import cc.twittertools.data.Tweet;
import cc.twittertools.data.TweetSet;
import cc.twittertools.eval.Evaluation;
import cc.twittertools.kde.Data;
import cc.twittertools.kde.WeightEstimation;
import cc.twittertools.kde.WeightKDE;

public class KDEModel extends Model {
	private static final int UniformWeight = 0;
	private static final int ScoreBasedWeight = 1;
	private static final int RankBasedWeight = 2;
	private static final int FeedbackWeight = 3;
	public static enum WeightEnum {UniformWeight, ScoreBasedWeight, RankBasedWeight, FeedbackWeight};
	
	//query id -> number of relevant results 
	private static Map<Integer, Integer> numrels;
	// query id -> top 10 relevant tweets
	private static Map<Integer, TweetSet> oracleSet;
	
	private static double[] MAP_ALPHA = new double[4];
	private static double[] P30_ALPHA = new double[4];
	public double[] EVAL_MAP = {0, 0, 0, 0, 0};
	public double[] EVAL_P30 = {0, 0, 0, 0, 0};
	private static double begin = 0, end = 1.0, stepSize = 0.02;
	
	@Override
	public void computeTMScore(TweetSet tweetSet, double lambda) {
		System.out.println("Empty Function, Exited");
		System.exit(0);
	}
	
	@Override
	public void computeTMScore(TweetSet tweetSet, TweetSet oracleSet,
			WeightEnum weightOption, double alpha) {
		DoubleArrayList weights = null;
		switch(weightOption){
		case UniformWeight: 
			weights = WeightEstimation.computeUniformWeights(tweetSet);
			break;
		case ScoreBasedWeight:
			weights = WeightEstimation.computeScoreBasedWeights(tweetSet);
			break;
		case RankBasedWeight: 
			weights = WeightEstimation.computeRankBasedWeights(tweetSet);
			break;
		case FeedbackWeight:
			weights = WeightEstimation.computeFeedbackWeights(tweetSet, oracleSet);
			break;
		default:
			throw new IllegalArgumentException("illegal weight option");
		}
		// Temporal Density Estimation
		DoubleArrayList times = new DoubleArrayList();
		for(Tweet tweet: tweetSet) {
			times.add(tweet.getTimeDiff()*1.0/TIME_INTERVAL);
		}
		Data data = new Data(times.toDoubleArray(), weights.toDoubleArray());
		data.computeStatistics();
		NormalDist kern = new NormalDist();
		double[] densities = WeightKDE.computeDensity(data, kern, times.toDoubleArray());
		
		// Temporal Integration
		for (int i = 0; i < tweetSet.size(); i++) {
			Tweet tweet = tweetSet.getTweet(i);
			double qlScore = tweet.getQlScore();
			double density = densities[i];
			double tmScore = (1-alpha) * qlScore + alpha * Math.log(density);
			tweet.setTMScore(tmScore);
		}
	}
	
	@Override
	public void train(Map<Integer, TweetSet> query2TweetSet, Table<Integer, Long, Integer> qrels
			, Map<Integer, Integer> numrels) throws Exception {
		oracleSet = TweetSet.getOracleSet(query2TweetSet, qrels);
		PrintStream out = new PrintStream(System.out, true, "UTF-8");
		out.println("Training begins.");
		int iter = 0;
		int interval =(int) ((end - begin) / stepSize) + 1;
		double[][] map = new double[4][interval];
		double[][] p30 = new double[4][interval];
		
		// begin training
		for (double alpha = begin; alpha <= end; alpha += stepSize) {
			for (WeightEnum woption : WeightEnum.values()){
				map[woption.ordinal()][iter] = 0;
				for (int qid : query2TweetSet.keySet()) {
					if (!qrels.containsRow(qid)) {
						continue;
					}
					// compute temporal language model score for each tweet
					computeTMScore(query2TweetSet.get(qid), oracleSet.get(qid), woption, alpha);
					query2TweetSet.get(qid).sortByTMscore();
					map[woption.ordinal()][iter] += Evaluation.MAP(qid, query2TweetSet.get(qid), qrels, numrels);
					p30[woption.ordinal()][iter] += Evaluation.P30(qid, query2TweetSet.get(qid), qrels);
				}
			}
			
			for (WeightEnum woption : WeightEnum.values()){
				map[woption.ordinal()][iter] /= numOfquerys;
				p30[woption.ordinal()][iter] /= numOfquerys;
			}
			
			if (debug) {
				out.println(String.format("%.3f : %.4f %.4f %.4f %.4f (MAP)", alpha, map[0][iter], 
					map[1][iter], map[2][iter], map[3][iter]));
				out.println(String.format("%.3f : %.4f %.4f %.4f %.4f (P30)", alpha, p30[0][iter], 
						p30[1][iter], p30[2][iter], p30[3][iter]));
			}
			iter++;
		}
		
		// select the ALPHAs to maximize test statistics (MAP and P30)
		for (int woption = 0; woption <= 3; woption++) {
			double map_avg = 0, map_max = 0;
			double p30_avg = 0, p30_max = 0;
			for (int i = 0; i < interval; i++) {
				if (i == 0 || i == interval - 1) {
					continue;
				} else {
					map_avg = (map[woption][i-1] + map[woption][i] + map[woption][i+1]) / 3.0;
					p30_avg = (p30[woption][i-1] + p30[woption][i] + p30[woption][i+1]) / 3.0;
				}
				if (map_avg > map_max) {
					map_max = map_avg;
					MAP_ALPHA[woption] = i * stepSize + begin;
				}
				if (p30_avg > p30_max) {
					p30_max = p30_avg;
					P30_ALPHA[woption] = i * stepSize + begin;
				}
			}
		}
		
		out.println("Training ends.");
		out.println(String.format("MAP Optimal Alpha: %.3f %.3f %.3f %.3f", MAP_ALPHA[0], MAP_ALPHA[1], MAP_ALPHA[2], MAP_ALPHA[3]));
		out.println(String.format("P30 Optimal Alpha: %.3f %.3f %.3f %.3f", P30_ALPHA[0], P30_ALPHA[1], P30_ALPHA[2], P30_ALPHA[3]));
	}
	
	@Override
	public void test(Map<Integer, TweetSet> query2TweetSet, Table<Integer, Long, Integer> qrels
			, Map<Integer, Integer> numrels) throws Exception {
		oracleSet = TweetSet.getOracleSet(query2TweetSet, qrels);
		PrintStream out = new PrintStream(System.out, true, "UTF-8");
		out.println("----------------------------------");
		out.println("Testing.");
		double[][] map_per_query = new double[5][numOfquerys];
		double[][] p30_per_query = new double[5][numOfquerys];
		EVAL_MAP = new double[]{0, 0, 0, 0, 0};
		EVAL_P30 = new double[]{0, 0, 0, 0, 0};
		P30_ALPHA = MAP_ALPHA ;//= new double[]{0.32, 0.38, 0.08, 0.40};
		for (int woption = 4; woption >= 0; woption--){ // woption is weight option
			int counter = 0;
			for (int qid : query2TweetSet.keySet()) {
				if (!qrels.containsRow(qid)) {
					continue;
				}
				if (woption == 4) { // baseline;
					computeTMScore(query2TweetSet.get(qid), null, WeightEnum.UniformWeight, 0);
					query2TweetSet.get(qid).sortByTMscore();
					map_per_query[woption][counter] = Evaluation.MAP(qid, query2TweetSet.get(qid), qrels, numrels);
					p30_per_query[woption][counter] = Evaluation.P30(qid, query2TweetSet.get(qid), qrels);
				} else {
					computeTMScore(query2TweetSet.get(qid), 
							oracleSet.get(qid), WeightEnum.values()[woption], MAP_ALPHA[woption]);
					query2TweetSet.get(qid).sortByTMscore();
					map_per_query[woption][counter] = Evaluation.MAP(qid, query2TweetSet.get(qid), qrels, numrels);
					computeTMScore(query2TweetSet.get(qid), 
							oracleSet.get(qid), WeightEnum.values()[woption], MAP_ALPHA[woption]);
					query2TweetSet.get(qid).sortByTMscore();
					p30_per_query[woption][counter] = Evaluation.P30(qid, query2TweetSet.get(qid), qrels);
				}
				EVAL_MAP[woption] += map_per_query[woption][counter];
				EVAL_P30[woption] += p30_per_query[woption][counter];
				counter++;
			}
		}
		
		for (int woption = 0; woption < 5; woption++){
			EVAL_MAP[woption] /= numOfquerys;
			EVAL_P30[woption] /= numOfquerys;
		}
		
		out.println(String.format("MAP: %.4f %.4f %.4f %.4f %.4f", EVAL_MAP[4], EVAL_MAP[0], EVAL_MAP[1], EVAL_MAP[2], EVAL_MAP[3]));
		out.println(String.format("P30: %.4f %.4f %.4f %.4f %.4f", EVAL_P30[4], EVAL_P30[0], EVAL_P30[1], EVAL_P30[2], EVAL_P30[3]));
		
		if (per_query) {
			writeQueryResults(map_per_query, p30_per_query);
		}
	}
	
	public void writeQueryResults(double[][] map_per_query, double[][] p30_per_query) throws IOException {
		String options[] = {"kde.uniform", "kde.score", "kde.rank", "kde.oracle", "kde.baseline"};
		for(int woption = 0; woption < 5; woption++) {
			String fileName = outputDir + options[woption] + ".perquery.txt";
			BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
			for (int qcnt = 0; qcnt < map_per_query[woption].length; qcnt++) {
				int qid = 2 * qcnt + 1;
				bw.write("map " + qid +" " + map_per_query[woption][qcnt] + "\n");
				bw.write("P_30 " + qid +" " + p30_per_query[woption][qcnt] + "\n");
			}
			bw.close();
		}
	}
	
	@Override
	public double[] getMAParray() {
		return EVAL_MAP;
	}
	
	@Override
	public double[] getP30array() {
		return EVAL_P30;
	}
}
