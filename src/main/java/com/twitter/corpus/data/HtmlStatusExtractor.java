package com.twitter.corpus.data;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;

import com.google.common.base.Preconditions;

public class HtmlStatusExtractor {
  private final Pattern TWEET_PATTERN =
      Pattern.compile("<p[^>]*class=\"[^\"]*js-tweet-text[^\"]*tweet-text[^\"]*\"[^>]*>(([^<]|(<([^/]|/[^p])[^>]*>))*)</p>");

  private final Pattern URL_PATTERN =
      Pattern.compile("<a[^>]*(data-expanded-url|href)=\"([^\"]*)\"[^>]*(href|data-expanded-url)=\"([^\"]*)\"[^>]*>(([^<]|(<([^/]|/[^a])[^>]*>))*)</a>");

  private final Pattern SCREENNAME_TIMESTAMP_PATTERN = 
      Pattern.compile("<a[^>]*href=\"/([^\"/]*)/status/([0-9]*)\"[^>]*>[^<]*<span[^>]*class=\"[^\"]*timestamp[^\"]*\"[^>]*data-time=\"([0-9]*)\"[^>]*>");

  public HtmlStatusExtractor() {}

  public String extractTweet(String html) {
    Preconditions.checkNotNull(html);
    Matcher matcher = TWEET_PATTERN.matcher(html);
    if (!matcher.find()) {
      return null;
    }
    String tweet = matcher.group(1);
    matcher = URL_PATTERN.matcher(tweet);
    StringBuffer rawsb = new StringBuffer();
    while (matcher.find()) {
      HashMap<String,String> urls = new HashMap<String,String>();
      urls.put(matcher.group(1), matcher.group(2));
      urls.put(matcher.group(3), matcher.group(4));
      String deu = urls.containsKey("data-expanded-url") ? urls.get("data-expanded-url") : "";
      String href = urls.containsKey("href") ? urls.get("href") : "";
      if (!deu.equals(""))
        matcher.appendReplacement(rawsb, deu.replaceAll("\\$", "\\\\\\$"));
      else if (!href.equals(""))
        matcher.appendReplacement(rawsb, href.replaceAll("\\$", "\\\\\\$"));
      else
        matcher.appendReplacement(rawsb, "$0"); // don't replace this URL
    }
    matcher.appendTail(rawsb);
    tweet = rawsb.toString();
    tweet = tweet.replaceAll("<(.|\n)*?>", "").replaceAll("[\n\r]", "").replaceAll("[\t]", " ").trim();
    tweet = StringEscapeUtils.unescapeHtml(tweet);
    return tweet;
  }

  public String extractTimestamp(String html, long id) {
    Preconditions.checkNotNull(html);
    Matcher matcher = SCREENNAME_TIMESTAMP_PATTERN.matcher(html);
    // most tweets have a single timestamp in the HTML
    // retweets ONLY have the retweet timestamp in the HTML
    // replies have both the reply tweet and the original tweet's timestamps in
    //   the HTML, so we have to check the timestamp id
    String timestamp = null;
    while (matcher.find()) {
      timestamp = matcher.group(3);
      long id_i = Long.parseLong(matcher.group(2));
      if (id_i == id) {
        return timestamp;
      }
    }
    return timestamp;
  }
}
