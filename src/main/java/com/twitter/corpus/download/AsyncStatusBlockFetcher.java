package com.twitter.corpus.download;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.log4j.Logger;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Response;
import com.ning.http.client.AsyncHttpClientConfig.Builder;
import com.ning.http.client.extra.ThrottleRequestFilter;

public class AsyncStatusBlockFetcher {
  private static final Logger LOG = Logger.getLogger(AsyncStatusBlockFetcher.class);

  private File file;
  private String output;
  private ConcurrentSkipListMap<Long, String> tweets = new ConcurrentSkipListMap<Long, String>();

  private final AsyncHttpClient asyncHttpClient;

  private static String prefix;

  private int timeout = 5000;
  private int connections = 100;

  public AsyncStatusBlockFetcher(File file, String output) {
    Builder builder = new AsyncHttpClientConfig.Builder();
    builder.setConnectionTimeoutInMs(timeout)
      .setRequestTimeoutInMs(timeout)
      .setMaximumConnectionsTotal(connections)
      .addRequestFilter(new ThrottleRequestFilter(connections));

    this.asyncHttpClient = new AsyncHttpClient();
    this.file = file;
    this.output = output;
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
        String url = String.format("%s/1/statuses/show/%s.json", prefix, id);
        asyncHttpClient.prepareGet(url).execute(new TweetFetcherHandler(id, arr[1]));

        cnt++;

        if ( cnt % 500 == 0 ) {
          LOG.info(cnt + " requests submitted");
          try{
            Thread.sleep(5000);
          } catch (Exception e) {
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }    

    try{
      Thread.sleep(2 * timeout);
    } catch (Exception e) {
    }
    
    asyncHttpClient.close();

    long end = System.currentTimeMillis();
    long duration = end - start;
    LOG.info("Total request submitted: " + cnt);
    LOG.info(tweets.size() + " tweets fetched in " + duration + "ms");

    LOG.info("Writing tweets...");
    int written = 0;
    FileWriter out = new FileWriter(new File(output));
    for ( Map.Entry<Long, String> entry : tweets.entrySet()) {
      if (entry.getValue().matches("\\s*")) {
        continue;
      }
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
        tweets.put(id, s);
      } else if (response.getStatusCode() >= 500) {
        LOG.warn("Error status " + response.getStatusCode() + ": " + id);
        String url = String.format("%s/1/statuses/show/%s.json", prefix, id);
        LOG.warn("Resubmitting: " + url);
        asyncHttpClient.prepareGet(url).execute(new TweetFetcherHandler(id, username));
      }

      return response;
    }

    @Override
    public void onThrowable(Throwable t) {
      String url = String.format("%s/1/statuses/show/%s.json", prefix, id);

      LOG.warn("Error: " + t);
      LOG.warn("Resubmitting: " + url);
      try {
        asyncHttpClient.prepareGet(url).execute(new TweetFetcherHandler(id, username));
      } catch (IOException e) {
        // Give up...
        e.printStackTrace();
      }
    }
  }
  
  public final static void main(String[] args) throws Exception {
    prefix = args[0];
    new AsyncStatusBlockFetcher(new File(args[1]), args[2]).fetch();
  }
}
