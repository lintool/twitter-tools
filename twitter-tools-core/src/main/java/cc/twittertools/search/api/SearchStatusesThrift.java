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

import java.io.PrintStream;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cc.twittertools.thrift.gen.TResult;

public class SearchStatusesThrift {
  // Defaults: if user doesn't specify an actual query, run MB01 as a demo.
  private static final String DEFAULT_QID = "MB01";
  private static final String DEFAULT_Q = "BBC World Service staff cuts";
  private static final long DEFAULT_MAX_ID = 34952194402811905L;
  private static final int DEFAULT_NUM_RESULTS = 10;
  private static final String DEFAULT_RUNTAG = "lucene4lm";

  private static final String HELP_OPTION = "h";
  private static final String HOST_OPTION = "host";
  private static final String PORT_OPTION = "port";
  private static final String QID_OPTION = "qid";
  private static final String QUERY_OPTION = "q";
  private static final String RUNTAG_OPTION = "runtag";
  private static final String MAX_ID_OPTION = "max_id";
  private static final String NUM_RESULTS_OPTION = "num_results";
  private static final String GROUP_OPTION = "group";
  private static final String TOKEN_OPTION = "token";
  private static final String VERBOSE_OPTION = "verbose";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(HELP_OPTION, "show help"));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("host").create(HOST_OPTION));
    options.addOption(OptionBuilder.withArgName("port").hasArg()
        .withDescription("port").create(PORT_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("query id").create(QID_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("query text").create(QUERY_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("runtag").create(RUNTAG_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("maxid").create(MAX_ID_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of results to return").create(NUM_RESULTS_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("group id").create(GROUP_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("access token").create(TOKEN_OPTION));
    options.addOption(new Option(VERBOSE_OPTION, "print out complete document"));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (cmdline.hasOption(HELP_OPTION) || !cmdline.hasOption(HOST_OPTION)
        || !cmdline.hasOption(PORT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(SearchStatusesThrift.class.getName(), options);
      System.exit(-1);
    }

    String qid = cmdline.hasOption(QID_OPTION) ?
        cmdline.getOptionValue(QID_OPTION) : DEFAULT_QID;
    String query = cmdline.hasOption(QUERY_OPTION) ?
        cmdline.getOptionValue(QUERY_OPTION) : DEFAULT_Q;
    String runtag = cmdline.hasOption(RUNTAG_OPTION) ?
        cmdline.getOptionValue(RUNTAG_OPTION) : DEFAULT_RUNTAG;
    long maxId = cmdline.hasOption(MAX_ID_OPTION) ?
        Long.parseLong(cmdline.getOptionValue(MAX_ID_OPTION)) : DEFAULT_MAX_ID;
    int numResults = cmdline.hasOption(NUM_RESULTS_OPTION) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_RESULTS_OPTION)) : DEFAULT_NUM_RESULTS;
    boolean verbose = cmdline.hasOption(VERBOSE_OPTION);

    String group = cmdline.hasOption(GROUP_OPTION) ? cmdline.getOptionValue(GROUP_OPTION) : null;
    String token = cmdline.hasOption(TOKEN_OPTION) ? cmdline.getOptionValue(TOKEN_OPTION) : null;
    TrecSearchThriftClient client = new TrecSearchThriftClient(cmdline.getOptionValue(HOST_OPTION),
        Integer.parseInt(cmdline.getOptionValue(PORT_OPTION)), group, token);

    System.err.println("qid: " + qid);
    System.err.println("q: " + query);
    System.err.println("max_id: " + maxId);
    System.err.println("num_results: " + numResults);

    PrintStream out = new PrintStream(System.out, true, "UTF-8");

    List<TResult> results = client.search(query, maxId, numResults);
    int i = 1;
    for (TResult result : results) {
      out.println(String.format("%s Q0 %d %d %f %s", qid, result.id, i, result.rsv, runtag));
      if (verbose) {
        System.out.println("# " + result.toString().replaceAll("[\\n\\r]+", " "));
      }
      i++;
    }
    out.close();
  }
}