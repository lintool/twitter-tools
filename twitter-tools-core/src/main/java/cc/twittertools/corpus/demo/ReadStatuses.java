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
import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import cc.twittertools.corpus.data.JsonStatusBlockReader;
import cc.twittertools.corpus.data.JsonStatusCorpusReader;
import cc.twittertools.corpus.data.Status;
import cc.twittertools.corpus.data.StatusStream;

/**
 * Sample program to illustrate how to work with {@link StatusStream}.
 */
public class ReadStatuses {
  private static final Logger LOG = Logger.getLogger(ReadStatuses.class);

  private ReadStatuses() {}

  private static final String INPUT_OPTION = "input";
  private static final String VERBOSE_OPTION = "verbose";
  private static final String DUMP_OPTION = "dump";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input directory or file").create(INPUT_OPTION));
    options.addOption(VERBOSE_OPTION, false, "print logging output every 10000 tweets");
    options.addOption(DUMP_OPTION, false, "dump statuses");

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

    int cnt = 0;
    Status status;
    while ((status = stream.next()) != null) {
      if (cmdline.hasOption(DUMP_OPTION)) {
        String text = status.getText();
        if (text != null) {
          text = text.replaceAll("\\s+", " ");
          text = text.replaceAll("\0", "");
        }
        out.println(String.format("%d\t%s\t%s\t%s", status.getId(), status.getScreenname(),
            status.getCreatedAt(), text));
      }
      cnt++;
      if ( cnt % 10000 == 0 && cmdline.hasOption(VERBOSE_OPTION)) {
        LOG.info(cnt + " statuses read");
      }
    }
    stream.close();
    LOG.info(String.format("Total of %s statuses read.", cnt));
  }
}
