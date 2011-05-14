package com.twitter.corpus.demo;

import java.io.File;
import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusBlockReader;
import com.twitter.corpus.data.StatusCorpusReader;
import com.twitter.corpus.data.StatusStream;

/**
 * Sample program to illustrate how to work with {@link StatusStream}.
 */
public class ReadStatuses {
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

    File file = new File(cmdline.getOptionValue(INPUT_OPTION));
    if (!file.exists()) {
      System.err.println("Error: " + file + " does not exist!");
      System.exit(-1);
    }

    PrintStream out = new PrintStream(System.out, true, "UTF-8");

    StatusStream stream;
    if (file.isDirectory()) {
      stream = new StatusCorpusReader(file);
    } else {
      stream = new StatusBlockReader(file);
    }

    int cnt = 0;
    Status status;
    while ((status = stream.next()) != null) {
      if (cmdline.hasOption(DUMP_OPTION)) {
        out.println(String.format("%d\t%s\t%s\t%s", status.getId(), status.getScreenname(),
            status.getCreatedAt(), status.getText().replaceAll("\\n", " ")));
      }
      cnt++;
      if ( cnt % 10000 == 0 && cmdline.hasOption(VERBOSE_OPTION)) {
        out.println(cnt + " statuses read");
      }
    }
    out.println(String.format("Total of %s statuses read.", cnt));
  }
}
