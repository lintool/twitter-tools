package cc.twittertools.search;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * Class for representing, reading, and brokering a set of indri queries.
 */
public class TrecTopicSet implements Iterable<TrecTopic>{
  private List<TrecTopic> queries = Lists.newArrayList();

  public TrecTopicSet() {
  }

  public void add(TrecTopic q) {
    queries.add(q);
  }

  @Override
  public Iterator<TrecTopic> iterator() {
    return queries.iterator();
  }

  private static final Pattern TOP_PATTERN = Pattern.compile("<top(.*?)</top>", Pattern.DOTALL);
  private static final Pattern NUM_PATTERN = Pattern.compile("<num> Number: (MB\\d+) </num>", Pattern.DOTALL);

  // This is used in TREC 2011
  private static final Pattern TITLE_PATTERN = Pattern.compile("<title>\\s*(.*?)\\s*</title>", Pattern.DOTALL);
  // This is used in TREC 2012
  private static final Pattern TITLE_PATTERN2 = Pattern.compile("<query>\\s*(.*?)\\s*</query>", Pattern.DOTALL);

  private static final Pattern TWEETTIME_PATTERN = Pattern.compile("<querytweettime>\\s*(\\d+)\\s*</querytweettime>", Pattern.DOTALL);

  public static TrecTopicSet fromFile(String fileName) throws Exception {
    String s = Joiner.on("\n").join(Files.readLines(new File(fileName), Charsets.UTF_8));
    TrecTopicSet queries = new TrecTopicSet();

    Matcher matcher = TOP_PATTERN.matcher(s);
    while (matcher.find()) {
      String top = matcher.group(0);
      //System.out.println(top);

      Matcher m = NUM_PATTERN.matcher(top);
      if (!m.find()) {
        throw new RuntimeException();
      }
      //System.out.println("--" + m.group(1));
      String id = m.group(1);
      if (id.matches("MB0\\d\\d")) {
        //System.out.println(id);
        id = id.replace("MB0", "MB");
        
      }

      m = TITLE_PATTERN.matcher(top);
      if (!m.find()) {
        m = TITLE_PATTERN2.matcher(top);
        if (!m.find()) {
          throw new RuntimeException();
        }
      }
      //System.out.println("--" + m.group(1));
      String text = m.group(1);

      m = TWEETTIME_PATTERN.matcher(top);
      if (!m.find()) {
        throw new RuntimeException();
      }
      //System.out.println("--" + m.group(1));
      long time = Long.parseLong(m.group(1));
      queries.add(new TrecTopic(id, text, time));
    }
    return queries;
  }
}
