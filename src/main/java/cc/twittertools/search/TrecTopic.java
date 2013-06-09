package cc.twittertools.search;

import com.google.common.base.Preconditions;

public class TrecTopic {
  private String query;
  private String id;
  private long time;

  public TrecTopic(String id, String query, long time) {
    this.id = Preconditions.checkNotNull(id);
    this.query = Preconditions.checkNotNull(query);
    this.time = time;
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
}
