package com.twitter.corpus.download;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusBlockReader;
import com.twitter.corpus.data.StatusCorpusReader;
import com.twitter.corpus.data.StatusStream;

public class VerifyStatusBlock {
  private static final Logger LOG = Logger.getLogger(VerifyStatusBlock.class);

  private VerifyStatusBlock() {}

  private static final String STATUSES_OPTION = "statuses";
  private static final String DATA_OPTION = "data";
  private static final String SUCCESS_OPTION = "success_output";
  private static final String FAILURE_OPTION = "failure_output";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("gzipped json-encoded statuses").create(STATUSES_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("data file with tweetids").create(DATA_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output file for success").create(SUCCESS_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output file for failure").create(FAILURE_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(STATUSES_OPTION) || !cmdline.hasOption(DATA_OPTION) ||
        !cmdline.hasOption(SUCCESS_OPTION) | !cmdline.hasOption(FAILURE_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(VerifyStatusBlock.class.getName(), options);
      System.exit(-1);
    }

    File file = new File(cmdline.getOptionValue(STATUSES_OPTION));
    if (!file.exists()) {
      System.err.println("Error: " + file + " does not exist!");
      System.exit(-1);
    }

    StatusStream stream;
    if (file.isDirectory()) {
      stream = new StatusCorpusReader(file);
    } else {
      stream = new StatusBlockReader(file);
    }

    Set<Long> ids = new HashSet<Long>();

    int cnt = 0;
    Status status;
    while ((status = stream.next()) != null) {
      ids.add(status.getId());
      cnt++;
    }
    System.out.println(String.format("Total of %d statuses read.", cnt));

    BufferedReader data = new BufferedReader(new InputStreamReader(
        new FileInputStream(cmdline.getOptionValue(DATA_OPTION))));

    FileWriter successOut = new FileWriter(new File(cmdline.getOptionValue(SUCCESS_OPTION)));
    FileWriter failureOut = new FileWriter(new File(cmdline.getOptionValue(FAILURE_OPTION)));

    int successCnt = 0;
    int failureCnt = 0;
    String line;
    while ((line = data.readLine()) != null) {
      String[] arr = line.split("\\t");
      long id = Long.parseLong(arr[0]);
      if (ids.contains(id)) {
        successOut.write(line + "\n");
        successCnt++;
      } else {
        //LOG.warn(String.format("Id %d not found", id));
        failureOut.write(line + "\n");
        failureCnt++;
      }
    }

    System.out.println(String.format("Total of %d status id written to %s.",
        successCnt, cmdline.getOptionValue(SUCCESS_OPTION)));
    System.out.println(String.format("Total of %d status id written to %s",
        failureCnt, cmdline.getOptionValue(FAILURE_OPTION)));

    successOut.close();
    failureOut.close();
    data.close();
  }
}
