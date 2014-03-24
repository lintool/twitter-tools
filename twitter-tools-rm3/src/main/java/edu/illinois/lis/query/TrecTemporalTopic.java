package edu.illinois.lis.query;

import com.google.common.base.Preconditions;

public class TrecTemporalTopic {
  private String query;
  private String id;
  private long time;
  private double epoch;

  public TrecTemporalTopic(String id, String query, long time, double epoch) {
    this.id = Preconditions.checkNotNull(id);
    this.query = Preconditions.checkNotNull(query);
    Preconditions.checkArgument(time > 0);
    this.time = time;
    Preconditions.checkArgument(epoch > 0);
    this.epoch = epoch;
  }

  public String getId() {
    return id;
  }

  public String getQuery() {
    return query;
  }

  public long getQueryTweetTime() {
    return time;
  }
  
  public double getEpoch() {
	  return epoch;
  }
}
