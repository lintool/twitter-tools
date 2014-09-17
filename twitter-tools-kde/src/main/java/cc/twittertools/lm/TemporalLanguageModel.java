package cc.twittertools.lm;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.media.j3d.Alpha;

import com.google.common.base.Charsets;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Table;
import com.google.common.io.Files;

import umontreal.iro.lecuyer.probdist.NormalDist;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import cc.twittertools.kde.Data;
import cc.twittertools.kde.WeightEstimation;
import cc.twittertools.kde.WeightKDE;
import cc.twittertools.query.Tweet;
import cc.twittertools.query.TweetSet;

public class TemporalLanguageModel {
	private static double[] ALPHA = {0.66, 0.30, 0.14};
	private static final int RANK = 30;
	private static final int TIME_INTERVAL = 60*60*24;
	private static final int UniformWeight = 0;
	private static final int ScoreBasedWeight = 1;
	private static final int RankBasedWeight = 2;
	private static final int FeedbackWeight = 3;
	private static String metric = "P30";
	
	private static Table<Integer, Long, Integer> qrels;
	
	// load ground truth qrels
	public static void loadGroundTruth(String fileAddr) throws IOException {
		qrels = HashBasedTable.create();
		List<String> lines = Files.readLines(new File(fileAddr), Charsets.UTF_8);
		for(String line: lines) {
			String groups[] = line.split("\\s+");
			int qid = Integer.parseInt(groups[0]); // topic id
			long tweetid = Long.parseLong(groups[2]); // tweet id;
			int score = Integer.parseInt(groups[3]); // positive means relevant, otherwise irrelevant
			if (score > 0) {
				qrels.put(qid, tweetid, score);
			}
		}
	}
	
	public double P30 (int qid, TweetSet tweetSet) {
		// Compute Precision by Topic
		tweetSet.sortByTMscore();
		double P30 = 0;
		int rank = Math.min(RANK, tweetSet.size());
		for (int i = 0; i < rank; i++) {
			Tweet tweet = tweetSet.getTweet(i);
			if (qrels.contains(qid, tweet.getId())) {
				P30 += 1.0 / rank;
			}
		}
		return P30;
	}
	
	public double MAP (int qid, TweetSet tweetSet) {
		tweetSet.sortByTMscore();
		int TP = 0;
		double MAP = 0;
		for (int i = 0; i < tweetSet.size(); i++) {
			Tweet tweet = tweetSet.getTweet(i);
			if (qrels.contains(qid, tweet.getId())) {
				TP += 1;
			}
			MAP += TP * 1.0 / (i+1);
		}
		return MAP / tweetSet.size();
	}
	
	
	public TweetSet computeTLScore(int qid, TweetSet tweetSet, int weightOption, double alpha) {
		DoubleArrayList weights = null;
		switch(weightOption){
		case 0: 
			weights = WeightEstimation.computeUniformWeights(tweetSet);
			break;
		case 1:
			weights = WeightEstimation.computeScoreBasedWeights(tweetSet);
			break;
		case 2: 
			weights = WeightEstimation.computeRankBasedWeights(tweetSet);
			break;
		case 3:
			weights = WeightEstimation.computeFeedbackWeights(tweetSet, tweetSet);
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
		
		return tweetSet;
	}
	
	public void train(String searchResultFile, String qrelsFile) throws IOException {
		Map<Integer, TweetSet> query2TweetSet = TweetSet.fromFile(searchResultFile);
		loadGroundTruth(qrelsFile);
		PrintStream out = new PrintStream(System.out, true, "UTF-8");
		out.println("Training begins.");
		int numOfTopics = query2TweetSet.keySet().size();
		double maxPrecisions[] = {0, 0, 0};
		for (double alpha = 0; alpha <= 1; alpha += 0.02) {
			double[] precisions = {0, 0, 0};
			for (int qid : query2TweetSet.keySet()) {
				for (int weightOption = 0; weightOption <= 2; weightOption++){
					TweetSet tweetSet = computeTLScore(qid, query2TweetSet.get(qid), weightOption, alpha);
					if (metric.equals("MAP")) {
						precisions[weightOption] += MAP(qid, tweetSet);
					} else {
						precisions[weightOption] += P30(qid, tweetSet);
					}
				}
			}
			for (int weightOption = 0; weightOption <= 2; weightOption++){
				precisions[weightOption] /= numOfTopics;
			}
			out.println(String.format("%.3f : %.4f %.4f %.4f", alpha, precisions[0], precisions[1], precisions[2]));
			
			for (int i = 0; i < precisions.length; i++) {
				if (maxPrecisions[i] < precisions[i]) {
					maxPrecisions[i] = precisions[i];
					ALPHA[i] = alpha;
				}
			}
		}
		out.println("Training ends.");
		out.print(String.format("Optimal Alpha: %.2f %.2f %.2f", ALPHA[0], ALPHA[1], ALPHA[2]));
	}
	
	public void test(String searchResultFile, String qrelsFile) throws IOException {
		Map<Integer, TweetSet> query2TweetSet = TweetSet.fromFile(searchResultFile);
		loadGroundTruth(qrelsFile);
		PrintStream out = new PrintStream(System.out, true, "UTF-8");
		out.print("Testing: ");
		int numOfTopics = query2TweetSet.keySet().size();
		double precisions[] = {0, 0, 0, 0};
		for (int qid : query2TweetSet.keySet()) {
			for (int weightOption = 0; weightOption <= 2; weightOption++){
				TweetSet tweetSet = computeTLScore(qid, query2TweetSet.get(qid), weightOption, ALPHA[weightOption]);
				if (metric.equals("MAP")) {
					precisions[weightOption] += MAP(qid, tweetSet);
				} else {
					precisions[weightOption] += P30(qid, tweetSet);
				}
			}
		}
		for (int weightOption = 0; weightOption <= 2; weightOption++){
			precisions[weightOption] /= numOfTopics;
		}
		out.println(String.format("%.4f %.4f %.4f %.4f", precisions[0], 
				precisions[1], precisions[2], precisions[3]));
	}
	
	// don't need to look at this function
	public void load(String fileAddr) throws IOException {
		loadGroundTruth("data/qrels.training.txt");
		List<String> lines = Files.readLines(new File(fileAddr), Charsets.UTF_8);
		Map<Integer, TweetSet> query2TweetSet = new HashMap<Integer, TweetSet>();
		TweetSet tweetSet;
		int prevQid = 0;
		for (String line: lines) {
			String[] groups = line.split(" ");
			int qid = Integer.parseInt(groups[0].replace("MB", ""));
			long tweetId = Long.parseLong(groups[2]);
			double score = Double.parseDouble(groups[4]);
			if (prevQid != qid) {
				tweetSet = new TweetSet(qid);
				query2TweetSet.put(qid, tweetSet);
			}
			Tweet tweet = new Tweet(tweetId, score);
			tweet.setTMScore(score);
			tweetSet = query2TweetSet.get(qid);
			tweetSet.add(tweet);
			prevQid = qid;
		}
		
		double precisions = 0;
		for (int qid : query2TweetSet.keySet()) {
			tweetSet = query2TweetSet.get(qid);
			if (metric.equals("MAP")) {
				precisions += MAP(qid, tweetSet);
			} else {
				precisions += P30(qid, tweetSet);
			}
		}
		int numOfTopics = query2TweetSet.keySet().size();
		System.out.println(precisions/numOfTopics);
	}
	public static void main(String[] args) throws IOException {
		TemporalLanguageModel model = new TemporalLanguageModel();
		//model.load("data/miles.kde.training.txt");
		model.train(args[0], args[1]);
		model.test(args[2], args[3]);
	}
}
