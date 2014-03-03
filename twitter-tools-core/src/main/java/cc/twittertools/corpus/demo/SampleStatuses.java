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
import java.util.Random;

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

import cc.twittertools.corpus.data.JsonStatusBlockReader;
import cc.twittertools.corpus.data.JsonStatusCorpusReader;
import cc.twittertools.corpus.data.Status;
import cc.twittertools.corpus.data.StatusStream;
import cc.twittertools.index.IndexStatuses;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class SampleStatuses {
  private SampleStatuses() {
  }

  private static final String INPUT_OPTION = "input";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Random rand = new Random();

    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input directory or file").create(INPUT_OPTION));

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

    StatusStream stream;
    // Figure out if we're reading from HTML SequenceFiles or JSON.
    File file = new File(cmdline.getOptionValue(INPUT_OPTION));
    if (!file.exists()) {
      System.err.println("Error: " + file + " does not exist!");
      System.exit(-1);
    }

    if (file.isDirectory()) {
      stream = new JsonStatusCorpusReader(file);
    } else {
      stream = new JsonStatusBlockReader(file);
    }

    out.println("var tweets = [");
    int cnt = 0;
    Status status;
    while ((status = stream.next()) != null) {
      if (rand.nextInt(100) >= 10)
        continue;

      String text = status.getText();
      if (text != null) {
        text = text.replaceAll("\\s+", " ");
        text = text.replaceAll("\0", "");
      }

      List<String> tokens = parse(IndexStatuses.ANALYZER, text);
      if (tokens.size() < 5) {
        continue;
      }

      // remove quotes.
      String t = Joiner.on(" ").join(tokens).replaceAll("\"", "");
      out.println(String.format("{\"id_str\":\"%d\",\"text\":\"%s\"},", status.getId(), t));

      cnt++;
    }
    stream.close();
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
