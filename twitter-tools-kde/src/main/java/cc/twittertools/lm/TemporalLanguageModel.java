package cc.twittertools.lm;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
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
	private static double[] ALPHA = new double[3];
	private static final int RANK = 30;
	private static final int TIME_INTERVAL = 60*60*24;
	private static final int UniformWeight = 0;
	private static final int ScoreBasedWeight = 1;
	private static final int RankBasedWeight = 2;
	private static final int FeedbackWeight = 3;
	
	private static Table<Integer, Long, Integer> qrels;
	
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
	
	public double computeTopicPrecision(int qid, TweetSet tweetSet, int weightOption, double alpha) {
		DoubleArrayList weights = null;
		switch(weightOption){
		case 0: 
			weights = WeightEstimation.computeUniformWeights(tweetSet);
			break;
		case 1:
			weights = WeightEstimation.computeScoreBasedWeights(tweetSet);
			break;
		case 2: 
			double lambda = 2.0 / tweetSet.size();
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
		MinMaxPriorityQueue<Tweet> topTweets = tweetSet.topTweets(RANK);
		
		// Compute Precision by Topic
		double precision = 0;
		Iterator<Tweet> iterator = topTweets.iterator();
		while(iterator.hasNext()) {
			Tweet tweet = iterator.next();
			if (qrels.contains(qid, tweet.getId())) {
				precision += 1.0 / RANK;
			}
		}
		return precision;
	}
	
	public void train(String searchResultFile, String qrelsFile) throws IOException {
		Map<Integer, TweetSet> query2TweetSet = TweetSet.fromFile(searchResultFile);
		loadGroundTruth(qrelsFile);
		PrintStream out = new PrintStream(System.out, true, "UTF-8");
		int numOfTopics = query2TweetSet.keySet().size();
		double maxPrecisions[] = {0, 0, 0};
		for (double alpha = 0.1; alpha <= 1; alpha += 0.02) {
			double precisions[] = {0, 0, 0};
			for (int qid : query2TweetSet.keySet()) {
				precisions[0] += computeTopicPrecision(qid, query2TweetSet.get(qid), UniformWeight, alpha);
				precisions[1] += computeTopicPrecision(qid, query2TweetSet.get(qid), ScoreBasedWeight, alpha);
				precisions[2] += computeTopicPrecision(qid, query2TweetSet.get(qid), RankBasedWeight, alpha);
			}
			out.println(String.format("%.3f : %.4f %.4f %.4f", alpha, 
					precisions[0]/numOfTopics, precisions[1]/numOfTopics, precisions[2]/numOfTopics));
			for (int i = 0; i < precisions.length; i++) {
				if (maxPrecisions[i] < precisions[i]) {
					maxPrecisions[i] = precisions[i];
					ALPHA[i] = alpha;
				}
			}
		}
		out.print(String.format("Alpha: %.2f %.2f %.2f", ALPHA[0], ALPHA[1], ALPHA[2]));
	}
	
	public static void main(String[] args) throws IOException {
		TemporalLanguageModel model = new TemporalLanguageModel();
		model.train(args[0], args[1]);
	}
}
