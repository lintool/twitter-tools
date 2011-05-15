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
import com.ning.http.client.Response;
import com.twitter.corpus.data.Status;

public class AsyncStatusBlockFetcher {
  private static final Logger LOG = Logger.getLogger(AsyncStatusBlockFetcher.class);
  private static final int MAX_RETRY_ATTEMPTS = 3;

  private final File file;
  private final String output;
  private final AsyncHttpClient asyncHttpClient;
  private final ConcurrentSkipListMap<Long, String> tweets = new ConcurrentSkipListMap<Long, String>();
  private final ConcurrentSkipListMap<Long, Integer> retries = new ConcurrentSkipListMap<Long, Integer>();

  private static String prefix;

  public AsyncStatusBlockFetcher(File file, String output) {
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
    FileWriter out = new FileWriter(new File(output));
    for ( Map.Entry<Long, String> entry : tweets.entrySet()) {
      // Skip empty results.
//      if (entry.getValue().matches("\\s*")) {
//        continue;
//      }
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
          Status.fromJson(s);
        } catch (Exception e) {
          // If there's an exception, it means we got an incomplete JSON result. Try again.
          LOG.warn("Incomplete JSON status: " + id);
          String url = String.format("%s/1/statuses/show/%s.json", prefix, id);
          retry(url, id, username);

          return response;
        }

        tweets.put(id, s);
      } else if (response.getStatusCode() >= 500) {
        // Retry by submitting another request.

        LOG.warn("Error status " + response.getStatusCode() + ": " + id);
        String url = String.format("%s/1/statuses/show/%s.json", prefix, id);
        retry(url, id, username);
      }

      return response;
    }

    @Override
    public void onThrowable(Throwable t) {
      // Retry by submitting another request.

      LOG.warn("Error: " + t);
      String url = String.format("%s/1/statuses/show/%s.json", prefix, id);
      try {
        retry(url, id, username);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    private synchronized void retry(String url, long id, String username) throws IOException {
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
  
  public final static void main(String[] args) throws Exception {
    prefix = args[0];
    new AsyncStatusBlockFetcher(new File(args[1]), args[2]).fetch();
  }
}
