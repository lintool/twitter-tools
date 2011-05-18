package com.twitter.corpus.data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;

import com.google.common.base.Preconditions;

public class HtmlStatusExtractor {
  private final String TWEET_BEGIN_DELIMITER = "<span class=\"entry-content\">";
  private final String TWEET_END_DELIMITER = "<span class=\"meta entry-meta\"";

  private final Pattern TIMESTAMP_PATTERN =
      Pattern.compile("<span class=\"published timestamp\" data=\"\\{time:'([^']+)'\\}\">");

  public HtmlStatusExtractor() {}

  public String extractTweet(String html) {
    Preconditions.checkNotNull(html);
    int begin = html.indexOf(TWEET_BEGIN_DELIMITER);
    int end = html.indexOf(TWEET_END_DELIMITER);

    if (begin == -1 || end == -1) {
      return null;
    }

    String raw = html.substring(begin, end);
    raw = raw.replaceAll("<(.|\n)*?>", "").replaceAll("[\n\r]", "").replaceAll("[\t]", " ").trim();
    raw = StringEscapeUtils.unescapeHtml(raw);

    return raw;
  }

  public String extractTimestamp(String html) {
    Preconditions.checkNotNull(html);
    Matcher matcher = TIMESTAMP_PATTERN.matcher(html);
    if (!matcher.find()) {
      return null;
    }
    return matcher.group(1);
  }
}
