package com.twitter.corpus.download;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

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
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.twitter.corpus.data.HtmlStatus;
import com.twitter.corpus.demo.ReadStatuses;

import edu.umd.cloud9.io.pair.PairOfLongString;

public class AsyncHtmlStatusBlockCrawler {
  private static final Logger LOG = Logger.getLogger(AsyncHtmlStatusBlockCrawler.class);
  private static final int MAX_RETRY_ATTEMPTS = 3;
  private static final int TWEET_BLOCK_SIZE = 500;

  private final File file;
  private final String output;
  private final AsyncHttpClient asyncHttpClient;

  // Storing the number of retries.
  private final ConcurrentSkipListMap<Long, Integer> retries = new ConcurrentSkipListMap<Long, Integer>();

  // key = (statud id, username), value = StatusHtml object
  private final ConcurrentSkipListMap<PairOfLongString, HtmlStatus> crawl =
      new ConcurrentSkipListMap<PairOfLongString, HtmlStatus>();

  public AsyncHtmlStatusBlockCrawler(File file, String output) throws IOException {
    this.file = Preconditions.checkNotNull(file);
    this.output = Preconditions.checkNotNull(output);

    if (!file.exists()) {
      throw new IOException(file + " does not exist!");
    }

    this.asyncHttpClient = new AsyncHttpClient();
  }

  public static String getUrl(long id, String username) {
    Preconditions.checkNotNull(username);
    return String.format("http://twitter.com/%s/status/%d", username, id);
  }

  public void fetch() throws IOException {
    long start = System.currentTimeMillis();
    LOG.info("Processing " + file);

    int cnt = 0;
    try {
      BufferedReader data = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
      String line;
      while ((line = data.readLine()) != null) {
        String[] arr = line.split("\t");
        long id = Long.parseLong(arr[0]);
        String username = arr[1];
        String url = getUrl(id, username);
        asyncHttpClient.prepareGet(url).execute(new TweetFetcherHandler(id, username, url, false));

        cnt++;

        if (cnt % TWEET_BLOCK_SIZE == 0) {
          LOG.info(cnt + " requests submitted");
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Wait for the last requests to complete.
    try {
      Thread.sleep(10000);
    } catch (Exception e) {
      e.printStackTrace();
    }

    asyncHttpClient.close();

    long end = System.currentTimeMillis();
    long duration = end - start;
    LOG.info("Total request submitted: " + cnt);
    LOG.info(crawl.size() + " tweets fetched in " + duration + "ms");

    LOG.info("Writing tweets...");
    int written = 0;
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    SequenceFile.Writer out = SequenceFile.createWriter(fs, conf, new Path(output),
        PairOfLongString.class, HtmlStatus.class, SequenceFile.CompressionType.BLOCK);

    for (Map.Entry<PairOfLongString, HtmlStatus> entry : crawl.entrySet()) {
      written++;
      out.append(entry.getKey(), entry.getValue());
    }
    out.close();

    LOG.info(written + " statuses written.");
    LOG.info("Done!");
  }

  private class TweetFetcherHandler extends AsyncCompletionHandler<Response> {
    private final long id;
    private final String username;
    private final String url;
    private final boolean isRedirect;

    public TweetFetcherHandler(long id, String username, String url, boolean isRedirect) {
      this.id = id;
      this.username = username;
      this.url = url;
      this.isRedirect = isRedirect;
    }

    @Override
    public Response onCompleted(Response response) throws Exception {
      if (response.getStatusCode() >= 500) {
        // Retry by submitting another request.
        LOG.warn("Error status " + response.getStatusCode() + ": " + url);
        retry();

        return response;
      }

      if (response.getStatusCode() == 302) {
        String redirect = response.getHeader("Location");

        asyncHttpClient.prepareGet(redirect)
          .execute(new TweetFetcherHandler(id, username, redirect, true));

        return response;
      }

      crawl.put(new PairOfLongString(id, username),
          new HtmlStatus((isRedirect ? 302 : response.getStatusCode()), System.currentTimeMillis(),
              response.getResponseBody("UTF-8")));

      return response;
    }

    @Override
    public void onThrowable(Throwable t) {
      // Retry by submitting another request.

      LOG.warn("Error: " + t);
      try {
        retry();
      } catch (Exception e) {
        // Ignore silently.
      }
    }

    private synchronized void retry() throws Exception {
      // Wait before retrying.
      Thread.sleep(1000);

      if (!retries.containsKey(id)) {
        retries.put(id, 1);
        LOG.warn("Retrying: " + url + " attempt 1");
        asyncHttpClient.prepareGet(url).execute(
            new TweetFetcherHandler(id, username, url, isRedirect));
        return;
      }

      int attempts = retries.get(id);
      if (attempts > MAX_RETRY_ATTEMPTS) {
        LOG.warn("Abandoning: " + url + " after max retry attempts");
        return;
      }

      attempts++;
      LOG.warn("Retrying: " + url + " attempt " + attempts);
      asyncHttpClient.prepareGet(url).execute(
          new TweetFetcherHandler(id, username, url, isRedirect));
      retries.put(id, attempts);
    }
  }

  private static final String DATA_OPTION = "data";
  private static final String OUTPUT_OPTION = "output";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("data file with tweet ids").create(DATA_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output file").create(OUTPUT_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(DATA_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(ReadStatuses.class.getName(), options);
      System.exit(-1);
    }

    String output = cmdline.getOptionValue(OUTPUT_OPTION);
    new AsyncHtmlStatusBlockCrawler(new File(cmdline.getOptionValue(DATA_OPTION)), output).fetch();
  }
}
