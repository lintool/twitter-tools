package com.twitter.corpus.download;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.twitter.corpus.data.HtmlStatus;

import edu.umd.cloud9.io.pair.PairOfLongString;

public class VerifyHtmlStatusBlockCrawl {
  private static final Logger LOG = Logger.getLogger(VerifyHtmlStatusBlockCrawl.class);

  private final File data;
  private final Path statuses;
  private final AsyncHttpClient client = new AsyncHttpClient();
  private final FileSystem fs;

  private File outputSuccess = null;
  private File outputFailure = null;
  private Path repairedOutput = null;

  public VerifyHtmlStatusBlockCrawl(File data, Path statuses, FileSystem fs) throws IOException {
    this.data = Preconditions.checkNotNull(data);
    this.statuses = Preconditions.checkNotNull(statuses);
    this.fs = Preconditions.checkNotNull(fs);

    if (!fs.exists(statuses)) {
      throw new RuntimeException(statuses + " does not exist!");
    }

    if (fs.getFileStatus(statuses).isDir()) {
      throw new RuntimeException(statuses + " does not exist!");
    }
  }

  public VerifyHtmlStatusBlockCrawl withOutputSuccess(File file) {
    this.outputSuccess = Preconditions.checkNotNull(file);
    return this;
  }

  public VerifyHtmlStatusBlockCrawl withOutputFailure(File file) {
    this.outputFailure = Preconditions.checkNotNull(file);
    return this;
  }

  public VerifyHtmlStatusBlockCrawl withRepairedOutput(Path path) {
    this.repairedOutput = Preconditions.checkNotNull(path);
    return this;
  }

  public boolean verify() throws IOException {
    LOG.info(String.format("Reading statuses read from %s.", statuses));

    Map<PairOfLongString, HtmlStatus> crawl = Maps.newTreeMap();

    SequenceFile.Reader reader = new SequenceFile.Reader(fs, statuses, fs.getConf());
    PairOfLongString key = new PairOfLongString();
    HtmlStatus value = new HtmlStatus();

    int cnt = 0;
    while (reader.next(key, value)) {
      crawl.put(key.clone(), value.clone());
      cnt++;
    }
    reader.close();
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
    String line;
    while ((line = in.readLine()) != null) {
      String[] arr = line.split("\\t");
      long id = Long.parseLong(arr[0]);
      String username = arr[1];
      totalCnt++;

      if (crawl.containsKey(new PairOfLongString(id, username))) {
        if (successOut != null) {
          successOut.write(line + "\n");
        }
        successCnt++;
      } else {
        // Check to see if we should actually bother repairing.
        if (repairedOutput != null) {

          Response response = fetchUrl(AsyncHtmlStatusBlockCrawler.getUrl(id, username));
          if (response.getStatusCode() == 302) {
            String redirect = response.getHeader("Location");
            response = fetchUrl(redirect);

            crawl.put(new PairOfLongString(id, username),
                new HtmlStatus(302, System.currentTimeMillis(), response.getResponseBody("UTF-8")));
          } else {
            // Status 200 = okay
            // Status 4XX = delete, forbid, etc. add a tombstone.
            crawl.put(new PairOfLongString(id, username),
                new HtmlStatus(200, System.currentTimeMillis(), response.getResponseBody("UTF-8")));
          }
          fetchedCnt++;
        }

        if (failureOut != null) {
          failureOut.write(line + "\n");
        }
        failureCnt++;
      }
    }

    LOG.info(String.format("%d missing statuses fetched.", fetchedCnt));

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
      SequenceFile.Writer repaired = SequenceFile.createWriter(fs, fs.getConf(), repairedOutput,
          PairOfLongString.class, HtmlStatus.class, SequenceFile.CompressionType.BLOCK);

      for (Map.Entry<PairOfLongString, HtmlStatus> entry : crawl.entrySet()) {
        written++;
        repaired.append(entry.getKey(), entry.getValue());
      }
      repaired.close();

      LOG.info(written + " statuses written.");
      LOG.info("Done!");
    }

    return true;
  }

  private Response fetchUrl(String url) {
    Response response = null;
    while (true) {
      try {
        response = client.prepareGet(url).execute().get();

        if (response.getStatusCode() < 500) {
          break;
        }
      } catch (InterruptedException e) {
        // Do nothing, just retry.
      } catch (ExecutionException e) {
        // Do nothing, just retry.
      } catch (IOException e) {
        // Do nothing, just retry.
      }

      try {
        Thread.sleep(1000);
      } catch (Exception e) {
      }
      LOG.warn("Error: retrying.");
    }

    return response;
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
        .withDescription("inputd HTML statuses")
        .create(STATUSES_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("repaired HTML statuses")
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
      formatter.printHelp(VerifyHtmlStatusBlockCrawl.class.getName(), options);
      System.exit(-1);
    }

    FileSystem fs = FileSystem.get(new Configuration());
    VerifyHtmlStatusBlockCrawl v = new VerifyHtmlStatusBlockCrawl(new File(cmdline
        .getOptionValue(DATA_OPTION)),
        new Path(cmdline.getOptionValue(STATUSES_OPTION)), fs);

    if (cmdline.hasOption(OUTPUT_SUCCESS_OPTION)) {
      v.withOutputSuccess(new File(cmdline.getOptionValue(OUTPUT_SUCCESS_OPTION)));
    }

    if (cmdline.hasOption(OUTPUT_FAILURE_OPTION)) {
      v.withOutputFailure(new File(cmdline.getOptionValue(OUTPUT_FAILURE_OPTION)));
    }

    if (cmdline.hasOption(STATUSES_REPAIRED_OPTION)) {
      v.withRepairedOutput(new Path(cmdline.getOptionValue(STATUSES_REPAIRED_OPTION)));
    }

    v.verify();
  }
}
