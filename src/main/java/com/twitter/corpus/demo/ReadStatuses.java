package com.twitter.corpus.demo;

import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

    FileSystem fs = FileSystem.get(new Configuration());
    Path file = new Path(cmdline.getOptionValue(INPUT_OPTION));
    if (!fs.exists(file)) {
      System.err.println("Error: " + file + " does not exist!");
      System.exit(-1);
    }

    PrintStream out = new PrintStream(System.out, true, "UTF-8");

    StatusStream stream;
    if (fs.getFileStatus(file).isDir()) {
      stream = new StatusCorpusReader(file, fs);
    } else {
      stream = new StatusBlockReader(file, fs);
    }

    int cnt = 0;
    Status status;
    while ((status = stream.next()) != null) {
      if (cmdline.hasOption(DUMP_OPTION)) {
        String text = status.getText();
        if (text != null) {
          text = text.replaceAll("\\n", " ");
        }
        out.println(String.format("%d\t%s\t%d\t%s\t%s", status.getId(), status.getScreenname(),
            status.getHttpStatusCode(), status.getCreatedAt(), text));
      }
      cnt++;
      if ( cnt % 10000 == 0 && cmdline.hasOption(VERBOSE_OPTION)) {
        out.println(cnt + " statuses read");
      }
    }
    out.println(String.format("Total of %s statuses read.", cnt));
  }
}
