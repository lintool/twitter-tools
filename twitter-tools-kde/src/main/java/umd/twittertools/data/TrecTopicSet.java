/**
 * Twitter Tools
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package umd.twittertools.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
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

public class TrecTopicSet implements Iterable<TrecTopic>{
  private List<TrecTopic> queries = Lists.newArrayList();

  public TrecTopicSet() {}

  public void add(TrecTopic q) {
    queries.add(q);
  }
  
  public void addAll(TrecTopicSet set) {
  	queries.addAll(set.queries);
  }

  @Override
  public Iterator<TrecTopic> iterator() {
    return queries.iterator();
  }

  private static final Pattern TOP_PATTERN = Pattern.compile("<top(.*?)</top>", Pattern.DOTALL);
  private static final Pattern NUM_PATTERN = Pattern.compile("<num> Number: MB(\\d+) </num>", Pattern.DOTALL);

  // TREC 2011 topics uses <title> tag
  private static final Pattern TITLE_PATTERN = Pattern.compile("<title>\\s*(.*?)\\s*</title>", Pattern.DOTALL);
  // TREC 2012 topics use <query> tag
  private static final Pattern TITLE_PATTERN2 = Pattern.compile("<query>\\s*(.*?)\\s*</query>", Pattern.DOTALL);
  
  private static final Pattern TIME_PATTERN = Pattern.compile("<querytime>\\s*(.*?)\\s*</querytime>", Pattern.DOTALL);
  
  private static final Pattern TWEETTIME_PATTERN = Pattern.compile("<querytweettime>\\s*(\\d+)\\s*</querytweettime>", Pattern.DOTALL);
  
  private static final DateFormat dateFormat= new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
  
  public static TrecTopicSet fromFile(File f) throws Exception {
    Preconditions.checkNotNull(f);
    Preconditions.checkArgument(f.exists());

    String s = Joiner.on("\n").join(Files.readLines(f, Charsets.UTF_8));
    TrecTopicSet queries = new TrecTopicSet();

    Matcher matcher = TOP_PATTERN.matcher(s);
    while (matcher.find()) {
      String top = matcher.group(0);

      Matcher m = NUM_PATTERN.matcher(top);
      if (!m.find()) {
        throw new IOException("Error parsing " + f);
      }
      int id = Integer.parseInt(m.group(1));

      m = TITLE_PATTERN.matcher(top);
      if (!m.find()) {
        m = TITLE_PATTERN2.matcher(top);
        if (!m.find()) {
          throw new IOException("Error parsing " + f);
        }
      }
      String text = m.group(1);
      
      m = TIME_PATTERN.matcher(top);
      if (!m.find()) {
        throw new IOException("Error parsing " + f);
      }
      long topictime = dateFormat.parse(m.group(1)).getTime() / 1000;
      
      m = TWEETTIME_PATTERN.matcher(top);
      if (!m.find()) {
        throw new IOException("Error parsing " + f);
      }
      long tweettime = Long.parseLong(m.group(1));
      queries.add(new TrecTopic(id, text, topictime, tweettime));
    }
    return queries;
  }
  
  public static void writeBack(File outfile, TrecTopicSet outSet) throws IOException {
  	BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
  	for (TrecTopic topic : outSet) {
  		bw.write("<top>\n");
  		bw.write("<num> Number: MB" + topic.getId() + " </num>\n");
  		bw.write("<query> " + topic.getQuery() + " </query>\n");
  		bw.write("<querytime> " + topic.getQueryTime() + " </querytime>\n");
  		bw.write("<querytweettime> " + topic.getQueryTweetTime() + " </querytweettime>\n");
  		bw.write("</top>\n\n");
  	}
  	bw.close();
  }
}
