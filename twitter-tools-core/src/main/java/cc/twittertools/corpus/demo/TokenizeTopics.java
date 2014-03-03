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

package cc.twittertools.corpus.demo;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.search.TrecTopic;
import cc.twittertools.search.TrecTopicSet;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class TokenizeTopics {
  private TokenizeTopics() {
  }

  private static final String INPUT_OPTION = "input";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("topic file").create(INPUT_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(ReadStatuses.class.getName(), options);
      System.exit(-1);
    }

    PrintStream out = new PrintStream(System.out, true, "UTF-8");

    File file = new File(cmdline.getOptionValue(INPUT_OPTION));
    if (!file.exists()) {
      System.err.println("Error: " + file + " does not exist!");
      System.exit(-1);
    }

    TrecTopicSet topics = TrecTopicSet.fromFile(file);
    for ( TrecTopic topic : topics ) {
      List<String> tokens = parse(IndexStatuses.ANALYZER, topic.getQuery());

      // remove quotes.
      String t = Joiner.on(" ").join(tokens).replaceAll("\"", "");
      out.println(String.format("{\"id\":\"%s\",\"text\":\"%s\"},", topic.getId(), t));

    }
    out.println("];");
  }

  static public List<String> parse(Analyzer analyzer, String keywords) throws IOException {
    List<String> list = Lists.newArrayList();

    TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(keywords));
    CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
    tokenStream.reset();
    while (tokenStream.incrementToken()) {
      if (cattr.toString().length() == 0) {
        continue;
      }
      list.add(cattr.toString());
    }
    tokenStream.end();
    tokenStream.close();

    return list;
  }
}
