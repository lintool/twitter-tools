package com.twitter.corpus.download;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusBlockReader;
import com.twitter.corpus.data.StatusCorpusReader;
import com.twitter.corpus.data.StatusStream;

public class VerifyStatusBlock {
  private static final Logger LOG = Logger.getLogger(VerifyStatusBlock.class);

  private VerifyStatusBlock() {}

  private static final String STATUSES_OPTION = "statuses";
  private static final String STATUSES_REPAIRED_OPTION = "statuses_repaired";
  private static final String DATA_OPTION = "data";
  private static final String SUCCESS_OPTION = "output_success";
  private static final String FAILURE_OPTION = "output_failure";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("gzipped JSON-encoded statuses, output of the AsynchStatusBlockFetcher")
        .create(STATUSES_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output gzipped JSON-encoded statuses after repair")
        .create(STATUSES_REPAIRED_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("data file with tweet ids").create(DATA_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output file for tweet fetch successes").create(SUCCESS_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output file for tweet fetch failures").create(FAILURE_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(STATUSES_OPTION) || !cmdline.hasOption(DATA_OPTION) ||
        !cmdline.hasOption(SUCCESS_OPTION) | !cmdline.hasOption(FAILURE_OPTION) ||
        !cmdline.hasOption(STATUSES_REPAIRED_OPTION) ) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(VerifyStatusBlock.class.getName(), options);
      System.exit(-1);
    }

    AsyncHttpClient client = new AsyncHttpClient();

    File file = new File(cmdline.getOptionValue(STATUSES_OPTION));
    if (!file.exists()) {
      System.err.println("Error: " + file + " does not exist!");
      System.exit(-1);
    }

    LOG.info(String.format("Reading statuses read from %s.", file));

    StatusStream stream;
    if (file.isDirectory()) {
      stream = new StatusCorpusReader(file);
    } else {
      stream = new StatusBlockReader(file);
    }

    Map<Long, String> ids = new HashMap<Long, String>();

    int cnt = 0;
    Status status;
    while ((status = stream.next()) != null) {
      ids.put(status.getId(), status.getJsonString());
      cnt++;
    }
    LOG.info(String.format("Total of %d statuses read.", cnt, file));

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
      if (ids.containsKey(id)) {
        successOut.write(line + "\n");
        successCnt++;
      } else {
        LOG.warn(String.format("Missing tweet id %d", id));

        Response response;
        while (true) {
          response = client.prepareGet(
              AsyncStatusBlockFetcher.getUrl(AsyncStatusBlockFetcher.DEFAULT_URL_PREFIX, id, arr[1]))
              .execute().get();
          if (response.getStatusCode() < 500) {
            break;
          }

          try {
            Thread.sleep(1000);
          } catch (Exception e) {}
          LOG.warn("Error: retrying.");
        }

        String s = response.getResponseBody();
        if (isTweetNoLongerAvailable(s)) {
          LOG.info(String.format("Tweet id %d is no longer available. Status OKAY.", id));
        } else {
          LOG.info(String.format("Successfully fetched tweet id %d", id));
          ids.put(id, response.getResponseBody());
          //LOG.info(response.getResponseBody());
        }

        failureOut.write(line + "\n");
        failureCnt++;
      }
    }

    LOG.info(String.format("Total of %d status id written to %s.",
        successCnt, cmdline.getOptionValue(SUCCESS_OPTION)));
    LOG.info(String.format("Total of %d status id written to %s",
        failureCnt, cmdline.getOptionValue(FAILURE_OPTION)));

    successOut.close();
    failureOut.close();
    data.close();
    client.close();

    LOG.info("Writing tweets...");
    String output = cmdline.getOptionValue(STATUSES_REPAIRED_OPTION);
    int written = 0;
    OutputStreamWriter out =
      new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(output)));
    for ( Map.Entry<Long, String> entry : ids.entrySet()) {
      written++;
      out.write(entry.getValue() + "\n");
    }
    out.close();
    LOG.info(written + " statuses written.");
    LOG.info("Done!");
  }

  public static boolean isTweetNoLongerAvailable(String s) {
    return s.contains("Sorry, you are not authorized to see this status.") ||
        s.contains("No status found with that ID.");
  }
}
