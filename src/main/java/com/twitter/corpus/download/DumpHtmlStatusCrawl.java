package com.twitter.corpus.download;

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
import org.apache.hadoop.io.SequenceFile;

import com.twitter.corpus.data.HtmlStatus;

import edu.umd.cloud9.io.pair.PairOfLongString;

public class DumpHtmlStatusCrawl {
  private static final String INPUT_OPTION = "input";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input crawl SequenceFile").create(INPUT_OPTION));

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
      formatter.printHelp(DumpHtmlStatusCrawl.class.getName(), options);
      System.exit(-1);
    }

    Path path = new Path(cmdline.getOptionValue(INPUT_OPTION));
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

    PairOfLongString key = new PairOfLongString();
    HtmlStatus value = new HtmlStatus();

    PrintStream out = new PrintStream(System.out, true, "UTF-8");
    while (reader.next(key, value)) {
      out.println(String.format("========== %s ==========\n%s\n", key.toString(),
          value.toString()));
    }
    reader.close();
  }
}
