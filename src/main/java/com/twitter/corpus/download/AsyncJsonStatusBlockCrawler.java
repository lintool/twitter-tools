package com.twitter.corpus.download;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
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
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.twitter.corpus.data.Status;
import com.twitter.corpus.demo.ReadStatuses;

public class AsyncJsonStatusBlockCrawler {
  private static final Logger LOG = Logger.getLogger(AsyncJsonStatusBlockCrawler.class);
  private static final int MAX_RETRY_ATTEMPTS = 3;

  public static final String DEFAULT_URL_PREFIX = "http://twitter.com";

  // Change these values at your own risk.
  private static final int TWEET_BLOCK_SIZE = 500;
  private static final int TWEET_BLOCK_SLEEP = 5000;

  private final File file;
  private final String output;
  private final AsyncHttpClient asyncHttpClient;
  private final ConcurrentSkipListMap<Long, String> tweets = new ConcurrentSkipListMap<Long, String>();
  private final ConcurrentSkipListMap<Long, Integer> retries = new ConcurrentSkipListMap<Long, Integer>();
  private final String prefix;

  public AsyncJsonStatusBlockCrawler(File file, String output) throws IOException {
    this(file, output, DEFAULT_URL_PREFIX);
  }

  public AsyncJsonStatusBlockCrawler(File file, String output, String prefix) throws IOException {
    this.file = Preconditions.checkNotNull(file);
    this.output = Preconditions.checkNotNull(output);
    this.prefix = Preconditions.checkNotNull(prefix);

    if (!file.exists()) {
      throw new IOException(file + " does not exist!");
    }

    this.asyncHttpClient = new AsyncHttpClient();
  }

  public static String getUrl(String prefix, long id, String username) {
    Preconditions.checkNotNull(username);
    return String.format("%s/statuses/show/%s.json", prefix, id);
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
        String url = getUrl(prefix, id, username);
        asyncHttpClient.prepareGet(url).execute(new TweetFetcherHandler(id, username));

        cnt++;

        if ( cnt % TWEET_BLOCK_SIZE == 0 ) {
          LOG.info(cnt + " requests submitted");
          try{
            Thread.sleep(TWEET_BLOCK_SLEEP);
          } catch (Exception e) {
            e.printStackTrace();
          }
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
    LOG.info(tweets.size() + " tweets fetched in " + duration + "ms");

    LOG.info("Writing tweets...");
    int written = 0;
    OutputStreamWriter out =
      new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(output)));
    for ( Map.Entry<Long, String> entry : tweets.entrySet()) {
      written++;
      out.write(entry.getValue() + "\n");
    }
    out.close();
    LOG.info(written + " statuses written.");
    LOG.info("Done!");
  }

  private class TweetFetcherHandler extends AsyncCompletionHandler<Response> {
    private long id;
    private String username;

    public TweetFetcherHandler(long id, String username) {
      this.id = id;
      this.username = username;
    }

    @Override
    public Response onCompleted(Response response) throws Exception {
      if (response.getStatusCode() == 200) {
        String s = response.getResponseBody();

        if ( "".equals(s) ) {
          LOG.warn("Empty result: " + id);
          return response;
        }

        // Try to decode the JSON.
        try {
          Status.fromJson(s).getId();
        } catch (Exception e) {
          // If there's an exception, it means we got an incomplete JSON result. Try again.
          LOG.warn("Incomplete JSON status: " + id);
          String url = getUrl(prefix, id, username);
          retry(url, id, username);

          return response;
        }

        tweets.put(id, s);
      } else if (response.getStatusCode() >= 500) {
        // Retry by submitting another request.

        LOG.warn("Error status " + response.getStatusCode() + ": " + id);
        String url = getUrl(prefix, id, username);
        retry(url, id, username);
      }

      return response;
    }

    @Override
    public void onThrowable(Throwable t) {
      // Retry by submitting another request.

      LOG.warn("Error: " + t);
      String url = getUrl(prefix, id, username);
      try {
        retry(url, id, username);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    private synchronized void retry(String url, long id, String username) throws IOException {
      // Wait before retrying.
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
        e.printStackTrace();
      }

      if ( !retries.containsKey(id)) {
        retries.put(id, 1);
        LOG.warn("Retrying: " + url + " attempt 1");
        asyncHttpClient.prepareGet(url).execute(new TweetFetcherHandler(id, username));
        return;
      }

      int attempts = retries.get(id);
      if (attempts > MAX_RETRY_ATTEMPTS) {
        LOG.warn("Abandoning: " + url + " after max retry attempts");
        return;
      }

      attempts++;
      LOG.warn("Retrying: " + url + " attempt " + attempts);
      asyncHttpClient.prepareGet(url).execute(new TweetFetcherHandler(id, username));
      retries.put(id, attempts);
    }
  }

  private static final String URL_PREFIX_OPTION = "url_prefix";
  private static final String DATA_OPTION = "data";
  private static final String OUTPUT_OPTION = "output";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("url").hasArg()
        .withDescription("URL prefix (optional argument)").create(URL_PREFIX_OPTION));
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
    if ( !output.endsWith(".gz")) {
      output += ".gz";
      LOG.warn("Output file specified does not contain the .gz suffix. Appending automatically.");
    }

    if (cmdline.hasOption(URL_PREFIX_OPTION)) {
      new AsyncJsonStatusBlockCrawler(new File(cmdline.getOptionValue(DATA_OPTION)),
          output, cmdline.getOptionValue(URL_PREFIX_OPTION)).fetch();
    } else {
      new AsyncJsonStatusBlockCrawler(new File(cmdline.getOptionValue(DATA_OPTION)), output).fetch();
    }
  }
}
