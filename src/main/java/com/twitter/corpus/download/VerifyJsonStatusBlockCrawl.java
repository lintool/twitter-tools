package com.twitter.corpus.download;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.JsonStatusBlockReader;
import com.twitter.corpus.data.StatusStream;

public class VerifyJsonStatusBlockCrawl {
  private static final Logger LOG = Logger.getLogger(VerifyJsonStatusBlockCrawl.class);

  private final File data;
  private final File statuses;
  private final AsyncHttpClient client = new AsyncHttpClient();

  private File outputSuccess = null;
  private File outputFailure = null;
  private File repairedOutput = null;

  public VerifyJsonStatusBlockCrawl(File data, File statuses) {
    this.statuses = Preconditions.checkNotNull(statuses);
    this.data = Preconditions.checkNotNull(data);

    if (!statuses.exists()) {
      throw new RuntimeException(statuses + " does not exist!");
    }
  }

  public VerifyJsonStatusBlockCrawl withOutputSuccess(File file) {
    this.outputSuccess = Preconditions.checkNotNull(file);
    return this;
  }

  public VerifyJsonStatusBlockCrawl withOutputFailure(File file) {
    this.outputFailure = Preconditions.checkNotNull(file);
    return this;
  }

  public VerifyJsonStatusBlockCrawl withRepairedOutput(File file) {
    this.repairedOutput = Preconditions.checkNotNull(file);
    return this;
  }

  public boolean verify() throws IOException {
    LOG.info(String.format("Reading statuses read from %s.", statuses));

    StatusStream stream;
    if (statuses.isDirectory()) {
      throw new RuntimeException(statuses + " cannot be a directory!");
    }
    stream = new JsonStatusBlockReader(statuses);

    Map<Long, String> ids = new HashMap<Long, String>();

    int cnt = 0;
    Status status;
    while ((status = stream.next()) != null) {
      ids.put(status.getId(), status.getJsonString());
      cnt++;
    }
    LOG.info(String.format("Total of %d statuses read.", cnt));

    BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(data)));

    FileWriter successOut = null;
    if (outputSuccess != null) {
      successOut = new FileWriter(outputSuccess);
    }
    FileWriter failureOut = null;
    if (outputFailure != null) {
      failureOut = new FileWriter(outputFailure);
    }

    int totalCnt = 0;
    int successCnt = 0;
    int failureCnt = 0;
    int fetchedCnt = 0;
    int notAvailableCnt = 0;
    String line;
    while ((line = in.readLine()) != null) {
      String[] arr = line.split("\\t");
      long id = Long.parseLong(arr[0]);
      totalCnt++;

      if (ids.containsKey(id)) {
        if (successOut != null) {
          successOut.write(line + "\n");
        }
        successCnt++;
      } else {
        // Check to see if we should actually bother repairing.
        if (repairedOutput != null) {
          Response response = null;
          while (true) {
            try {
              response = client.prepareGet(
                  AsyncJsonStatusBlockCrawler.getUrl(
                      AsyncJsonStatusBlockCrawler.DEFAULT_URL_PREFIX, id, arr[1]))
                  .execute().get();

              if (response.getStatusCode() < 500) {
                break;
              }
            } catch (InterruptedException e) {
              // Do nothing, just retry.
            } catch (ExecutionException e) {
              // Do nothing, just retry.
            }

            try {
              Thread.sleep(1000);
            } catch (Exception e) {
            }
            LOG.warn("Error: retrying.");
          }

          String s = response.getResponseBody();
          if (isTweetNoLongerAvailable(s)) {
            LOG.info(String.format("Missing status %d: no longer available.", id));
            notAvailableCnt++;
          } else {
            LOG.info(String.format("Missing status %d: successfully fetched.", id));
            ids.put(id, response.getResponseBody());
            fetchedCnt++;
          }
        }
      
        if (failureOut != null) {
          failureOut.write(line + "\n");
        }
        failureCnt++;
      }
    }

    LOG.info(String.format("Total of %d statuses in %s.", cnt, statuses));
    LOG.info(String.format("Total of %d entries in %s.", totalCnt, data));
    LOG.info(String.format("%d statuses no longer available.", notAvailableCnt));
    LOG.info(String.format("%d missing statuses fetched.", fetchedCnt));

    if (cnt + notAvailableCnt + fetchedCnt == totalCnt) {
      LOG.info("SUCCESS! All statuses accounted for.");
    }

    if (outputSuccess != null) {
      LOG.info(String.format("Total of %d status id written to %s.", successCnt, outputSuccess));
      successOut.close();
    }
    if (outputFailure != null) {
      LOG.info(String.format("Total of %d status id written to %s", failureCnt, outputFailure));
      failureOut.close();
    }

    in.close();
    client.close();

    if (repairedOutput != null) {
      LOG.info("Writing tweets...");
      int written = 0;
      OutputStreamWriter out = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(repairedOutput)));
      for (Map.Entry<Long, String> entry : ids.entrySet()) {
        written++;
        out.write(entry.getValue() + "\n");
      }
      out.close();
      LOG.info(written + " statuses written.");
      LOG.info("Done!");
    }

    return true;
  }

  public static boolean isTweetNoLongerAvailable(String s) {
    return s.contains("Sorry, you are not authorized to see this status.") ||
        s.contains("No status found with that ID.") || s.equals("");
  }

  private static final String STATUSES_OPTION = "statuses_input";
  private static final String STATUSES_REPAIRED_OPTION = "statuses_repaired";
  private static final String DATA_OPTION = "data";
  private static final String OUTPUT_SUCCESS_OPTION = "output_success";
  private static final String OUTPUT_FAILURE_OPTION = "output_failure";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input JSON statuses")
        .create(STATUSES_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("repaired JSON statuses")
        .create(STATUSES_REPAIRED_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("data file with tweet ids").create(DATA_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output file for tweet fetch successes").create(OUTPUT_SUCCESS_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output file for tweet fetch failures").create(OUTPUT_FAILURE_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(STATUSES_OPTION) || !cmdline.hasOption(DATA_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(VerifyJsonStatusBlockCrawl.class.getName(), options);
      System.exit(-1);
    }

    VerifyJsonStatusBlockCrawl v = new VerifyJsonStatusBlockCrawl(new File(cmdline.getOptionValue(DATA_OPTION)),
        new File(cmdline.getOptionValue(STATUSES_OPTION)));

    if (cmdline.hasOption(OUTPUT_SUCCESS_OPTION)) {
      v.withOutputSuccess(new File(cmdline.getOptionValue(OUTPUT_SUCCESS_OPTION)));
    }

    if (cmdline.hasOption(OUTPUT_FAILURE_OPTION)) {
      v.withOutputFailure(new File(cmdline.getOptionValue(OUTPUT_FAILURE_OPTION)));
    }

    if (cmdline.hasOption(STATUSES_REPAIRED_OPTION)) {
      v.withRepairedOutput(new File(cmdline.getOptionValue(STATUSES_REPAIRED_OPTION)));
    }

    v.verify();
  }
}
