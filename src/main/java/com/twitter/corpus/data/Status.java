package com.twitter.corpus.data;

/**
 * Object representing a status.
 */
public class Status {
  private static final HtmlTweetExtractor extractor = new HtmlTweetExtractor();

  private long id;
  private String screenname;
  private String createdAt;
  private String text;
  private int httpStatusCode;

  protected Status() {}
  
  public long getId() {
    return id;
  }

  public String getText() {
    return text;
  }

  public String getCreatedAt() {
    return createdAt;
  }

  public String getScreenname() {
    return screenname;
  }

  public int getHttpStatusCode() {
    return httpStatusCode;
  }

  public static Status fromHtml(long id, String username, int httpStatus, String html) {
    Status status = new Status();

    status.id = id;
    status.screenname = username;
    status.httpStatusCode = httpStatus;
    status.text = extractor.extractTweet(html);
    status.createdAt = extractor.extractTimestamp(html);

    // TODO: Note that http status code 302 indicates a redirect, which indicates a retweet. I.e.,
    // the status redirects to the original retweeted status. Note that this is not currently
    // handled.

    return status;
  }
}
