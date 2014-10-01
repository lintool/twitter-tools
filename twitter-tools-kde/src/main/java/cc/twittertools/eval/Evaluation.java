package cc.twittertools.eval;

import java.util.Map;

import com.google.common.collect.Table;

import cc.twittertools.data.Tweet;
import cc.twittertools.data.TweetSet;

public class Evaluation {
	private static final int RANK = 30;
	
	public static double P30 (int qid, TweetSet tweetSet, 
			Table<Integer, Long, Integer> qrels) {
		// Compute Precision by query
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
	
	public static double MAP (int qid, TweetSet tweetSet, 
			Table<Integer, Long, Integer> qrels, Map<Integer, Integer> numrels) {
		int TP = 0;
		double MAP = 0;
		for (int i = 0; i < tweetSet.size(); i++) {
			Tweet tweet = tweetSet.getTweet(i);
			if (qrels.contains(qid, tweet.getId())) {
				TP += 1;
				MAP += TP * 1.0 / (i+1);
			}
		}
		return MAP / numrels.get(qid);
	}
}
