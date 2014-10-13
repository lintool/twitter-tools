package cc.twittertools.eval;

import java.util.Map;

import com.google.common.collect.Table;

import cc.twittertools.data.Tweet;
import cc.twittertools.data.TweetSet;

public class Evaluation {
	
	public static double P_RANK (int qid, TweetSet tweetSet, 
			Table<Integer, Long, Integer> qrels, int rank) {
		// Compute Precision by query
		double P30 = 0;
		rank = Math.min(rank, tweetSet.size());
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
