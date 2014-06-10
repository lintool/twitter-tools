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

package cc.twittertools.search.api;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cc.twittertools.search.TrecTopicSet;
import cc.twittertools.thrift.gen.TResult;

public class RunQueriesThrift {
  private static final String DEFAULT_RUNTAG = "lucene4lm";

  private static final String HOST_OPTION = "host";
  private static final String PORT_OPTION = "port";
  private static final String QUERIES_OPTION = "queries";
  private static final String NUM_RESULTS_OPTION = "num_results";
  private static final String GROUP_OPTION = "group";
  private static final String TOKEN_OPTION = "token";
  private static final String RUNTAG_OPTION = "runtag";
  private static final String VERBOSE_OPTION = "verbose";

  private RunQueriesThrift() {}

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("host").create(HOST_OPTION));
    options.addOption(OptionBuilder.withArgName("port").hasArg()
        .withDescription("port").create(PORT_OPTION));
    options.addOption(OptionBuilder.withArgName("file").hasArg()
        .withDescription("file containing topics in TREC format").create(QUERIES_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of results to return").create(NUM_RESULTS_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("group id").create(GROUP_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("access token").create(TOKEN_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("runtag").create(RUNTAG_OPTION));
    options.addOption(new Option(VERBOSE_OPTION, "print out complete document"));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(HOST_OPTION) || !cmdline.hasOption(PORT_OPTION)
        || !cmdline.hasOption(QUERIES_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(RunQueriesThrift.class.getName(), options);
      System.exit(-1);
    }

    String queryFile = cmdline.getOptionValue(QUERIES_OPTION);
    if (!new File(queryFile).exists()) {
      System.err.println("Error: " + queryFile + " doesn't exist!");
      System.exit(-1);
    }

    String runtag = cmdline.hasOption(RUNTAG_OPTION) ?
        cmdline.getOptionValue(RUNTAG_OPTION) : DEFAULT_RUNTAG;

    TrecTopicSet topicsFile = TrecTopicSet.fromFile(new File(queryFile));

    int numResults = 1000;
    try {
      if (cmdline.hasOption(NUM_RESULTS_OPTION)) {
        numResults = Integer.parseInt(cmdline.getOptionValue(NUM_RESULTS_OPTION));
      }
    } catch (NumberFormatException e) {
      System.err.println("Invalid " + NUM_RESULTS_OPTION + ": " + cmdline.getOptionValue(NUM_RESULTS_OPTION));
      System.exit(-1);
    }

    String group = cmdline.hasOption(GROUP_OPTION) ? cmdline.getOptionValue(GROUP_OPTION) : null;
    String token = cmdline.hasOption(TOKEN_OPTION) ? cmdline.getOptionValue(TOKEN_OPTION) : null;

    boolean verbose = cmdline.hasOption(VERBOSE_OPTION);

    PrintStream out = new PrintStream(System.out, true, "UTF-8");

    TrecSearchThriftClient client = new TrecSearchThriftClient(cmdline.getOptionValue(HOST_OPTION),
        Integer.parseInt(cmdline.getOptionValue(PORT_OPTION)), group, token);

    for (cc.twittertools.search.TrecTopic query : topicsFile) {
      List<TResult> results = client.search(query.getQuery(),
          query.getQueryTweetTime(), numResults);
      int i = 1;
      Set<Long> tweetIds = new HashSet<Long>();
      for (TResult result : results) {
        if (!tweetIds.contains(result.id)) {
          // The TREC official qrels don't have the "MB" prefix and trailing zeros, so we perform
          // this transformation so that trec_eval doesn't complain.
          String qid = query.getId().replaceFirst("^MB0*", "");
          tweetIds.add(result.id);
          out.println(String.format("%s Q0 %d %d %f %s", qid, result.id, i, result.rsv, runtag));
          if (verbose) {
            out.println("# " + result.toString().replaceAll("[\\n\\r]+", " "));
          }
          i++;
        }
      }
    }
    out.close();
  }
}
