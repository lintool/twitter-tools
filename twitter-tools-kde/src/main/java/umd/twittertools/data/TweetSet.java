package umd.twittertools.data;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cc.twittertools.thrift.gen.TResult;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Table;
import com.google.common.io.Files;

public class TweetSet implements Iterable<Tweet>{
	private String query;
	private int queryId;
	private List<Tweet> tweets = Lists.newArrayList();
	
	public TweetSet(int queryId) {
		this.queryId = queryId;
	}
	
	public TweetSet(String query, int queryId) {
		this.query = query;
		this.queryId = queryId;
	}
	
	public void add (Tweet tweet) {
		tweets.add(tweet);
	}
	
	public boolean contains(Tweet other) {
		for (Tweet tweet : tweets) {
			if (tweet.equals(other)){
				return true;
			}
		}
		return false;
	}
	
	@Override
  public Iterator<Tweet> iterator() {
    return tweets.iterator();
  }
	
	public void sortByQLscore() {
		Comparator<Tweet> comparator = new Comparator<Tweet>() {
			@Override
			public int compare(Tweet t1, Tweet t2) {
				double diff = t1.getQlScore() - t2.getQlScore();
				return (diff == 0) ? 0 : (diff > 0) ? -1 : 1;
			}
		};
		Collections.sort(tweets, comparator);
	}
	
	public void sortByTMscore() {
		Comparator<Tweet> comparator = new Comparator<Tweet>() {
			@Override
			public int compare(Tweet t1, Tweet t2) {
				double diff = t1.getTMScore() - t2.getTMScore();
				return (diff == 0) ? 0 : (diff > 0) ? -1 : 1;
			}
		};
		Collections.sort(tweets, comparator);
	}
	
	public MinMaxPriorityQueue<Tweet> topTweets(int rank) {
		Comparator<Tweet> comparator = new Comparator<Tweet>() {
			@Override
			public int compare(Tweet t1, Tweet t2) {
				double diff = t1.getTMScore() - t2.getTMScore();
				return (diff == 0) ? 0 : (diff > 0) ? -1 : 1;
			}
		};
		MinMaxPriorityQueue<Tweet> heap = MinMaxPriorityQueue
				.orderedBy(comparator)
				.maximumSize(rank)
				.create();
		for (Tweet tweet : tweets) {
			heap.add(tweet);
		}
		return heap;
	}
	
	public static Map<Integer, TweetSet> fromFile (String fileAddr) throws IOException {
		List<String> lines = Files.readLines(new File(fileAddr), Charsets.UTF_8);
		Map<Integer, TweetSet> query2TweetSet = new HashMap<Integer, TweetSet>();
		int prevQid = -1;
		for (String line : lines) {
			String[] groups = line.split(" ");
			Preconditions.checkArgument(groups.length == 6);
			Integer qid = Integer.parseInt(groups[0]);
			long tweetId = Long.parseLong(groups[1]);
			int rank = Integer.parseInt(groups[2]);
			long epoch = Long.parseLong(groups[3]);
			long timeDiff = Long.parseLong(groups[4]);
			double score = Double.parseDouble(groups[5]);
			if (!qid.equals(prevQid)) {
				query2TweetSet.put(qid, new TweetSet(qid));
			}
			Tweet tweet = new Tweet(tweetId, rank, epoch, timeDiff, score);
			TweetSet tweetSet = query2TweetSet.get(qid);
			tweetSet.add(tweet);
			query2TweetSet.put(qid, tweetSet);
			prevQid = qid;
		}
		
		return query2TweetSet;
	}
	
	public static Map<Integer, TweetSet> getOracleSet(Map<Integer, TweetSet> query2TweetSet, 
			Table<Integer, Long, Integer> qrels) {
		Map<Integer, TweetSet> oracleMap = new HashMap<Integer, TweetSet>();
		for (int qid : query2TweetSet.keySet()) {
			int counter = 0;
			TweetSet tweetSet = query2TweetSet.get(qid);
			TweetSet oracleSet = new TweetSet(qid);
			for (Tweet tweet : tweetSet) {
				if (qrels.contains(qid, tweet.getId())) {
					oracleSet.add(new Tweet(tweet));
					if (++counter == 10) break;
				}
			}
			oracleMap.put(qid, oracleSet);
		}
		return oracleMap;
	}
	
	public static TweetSet getOracleSet(int qid, TweetSet tweetSet, 
			Table<Integer, Long, Integer> qrels) {
		int counter = 0;
		TweetSet oracleSet = new TweetSet(qid);
		for (Tweet tweet : tweetSet) {
			if (qrels.contains(qid, tweet.getId())) {
				oracleSet.add(new Tweet(tweet));
				if (++counter == 10) break;
			}
		}
		return oracleSet;
	}
	
	public int size() {
		return tweets.size();
	}
	
	public String getQuery() {
		return query;
	}

	public int getQueryId() {
		return queryId;
	}
	
	public Tweet getTweet(int index) {
		return tweets.get(index);
	}
	
	public List<Tweet> getTweets() {
		return tweets;
	}
}
