package edu.illinois.lis.query;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TrecTemporalTopicSet implements Iterable<TrecTemporalTopic>{
  private List<TrecTemporalTopic> queries = Lists.newArrayList();

  private TrecTemporalTopicSet() {}

  private void add(TrecTemporalTopic q) {
    queries.add(q);
  }

  public Iterator<TrecTemporalTopic> iterator() {
    return queries.iterator();
  }

  private static final String DATE_FORMAT = "EEE MMM d k:m:s ZZZZZ yyyy"; //"Fri Mar 29 11:03:41 +0000 2013"; 

  private static final Pattern TOP_PATTERN = Pattern.compile("<top(.*?)</top>", Pattern.DOTALL);
  private static final Pattern NUM_PATTERN = Pattern.compile("<num> Number: (MB\\d+) </num>", Pattern.DOTALL);

  // TREC 2011 topics uses <title> tag
  private static final Pattern TITLE_PATTERN = Pattern.compile("<title>\\s*(.*?)\\s*</title>", Pattern.DOTALL);
  // TREC 2012 topics use <query> tag
  private static final Pattern TITLE_PATTERN2 = Pattern.compile("<query>\\s*(.*?)\\s*</query>", Pattern.DOTALL);

  private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("<querytime>\\s*(.*?)\\s*</querytime>", Pattern.DOTALL);

  private static final Pattern TWEETTIME_PATTERN = Pattern.compile("<querytweettime>\\s*(\\d+)\\s*</querytweettime>", Pattern.DOTALL);

  
  public static TrecTemporalTopicSet fromFile(File f) throws IOException {
    Preconditions.checkNotNull(f);
    Preconditions.checkArgument(f.exists());

    String s = Joiner.on("\n").join(Files.readLines(f, Charsets.UTF_8));
    TrecTemporalTopicSet queries = new TrecTemporalTopicSet();

    Matcher matcher = TOP_PATTERN.matcher(s);
    while (matcher.find()) {
      String top = matcher.group(0);

      
      Matcher m = NUM_PATTERN.matcher(top);
      if (!m.find()) {
        throw new IOException("Error parsing " + f);
      }
      String id = m.group(1);
      // Topics from 2012 are inconsistently numbered,
      // e.g., MB051 should match the qrels, which has MB51
      if (id.matches("MB0\\d\\d")) {
        id = id.replace("MB0", "MB");
      }

      m = TITLE_PATTERN.matcher(top);
      if (!m.find()) {
        m = TITLE_PATTERN2.matcher(top);
        if (!m.find()) {
          throw new IOException("Error parsing " + f);
        }
      }
      String text = m.group(1);

      m = TIMESTAMP_PATTERN.matcher(top);
      if (!m.find()) {
        throw new IOException("Error parsing " + f);
      }
      double epoch = -1.0;
      try {
          epoch = (new SimpleDateFormat(DATE_FORMAT)).parse(m.group(1)).getTime() / 1000;
        } catch (ParseException e) {
          epoch = -1.0;
        }
      
      m = TWEETTIME_PATTERN.matcher(top);
      if (!m.find()) {
        throw new IOException("Error parsing " + f);
      }
      long time = Long.parseLong(m.group(1));
      

      
      queries.add(new TrecTemporalTopic(id, text, time, epoch));
    }
    return queries;
  }
}
