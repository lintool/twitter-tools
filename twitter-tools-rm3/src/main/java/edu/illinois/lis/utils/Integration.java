package edu.illinois.lis.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import umd.twittertools.data.Tweet;
import umd.twittertools.data.TweetSet;
import cc.twittertools.thrift.gen.TResult;
import edu.illinois.lis.query.GQuery;

public class Integration {
	
	//private static QueryLikelihoodModel qlModel = new QueryLikelihoodModel();
	
	public static TweetSet TResultSet2TweetSet(GQuery query, List<TResult> results, boolean rmRetweet) {
		String queryText = query.getText();
		int queryId = Integer.parseInt(query.getTitle().replaceFirst("^MB0*", ""));
		long queryTime = (long)query.getEpoch();
		TweetSet tweetSet = new TweetSet(queryText, queryId);
		//qlModel.setCorpus(results); // query likelihood model set corpus.
		Set<Long> tweetIds = new HashSet<Long>();
		for (TResult result : results) {
			if (!tweetIds.contains(result.id)) {
				if (rmRetweet) {
					if (result.retweeted_status_id != 0) {
						continue; // filter all retweets
					}
				}
				tweetIds.add(result.id);
				// compute query likelihood score
				// double score = qlModel.computeScore(queryText, result.text);
				Tweet tweet = new Tweet(result.id, result.epoch, queryTime - result.epoch, result.rsv);
				tweet.setText(result.text);
				tweet.setRsv(result.rsv);
				tweetSet.add(tweet);
			}
		}
		tweetSet.sortByQLscore();
		return tweetSet;
	}
	
	public static List<TResult> TweetSet2TResultSet(List<Tweet> tweetSet) {
		List<TResult> results = new ArrayList<TResult>();
		Iterator<Tweet> iterator = tweetSet.iterator();
		while(iterator.hasNext()) {
			results.add(iterator.next().toResult());
		}
		return results;
	}
	
	public static List<TResult> TweetSet2TResultSet(TweetSet tweetSet) {
		List<TResult> results = new ArrayList<TResult>();
		Iterator<Tweet> iterator = tweetSet.iterator();
		while(iterator.hasNext()) {
			results.add(iterator.next().toResult());
		}
		return results;
	}
}
