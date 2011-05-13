package com.twitter.academia.trec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

public class AsyncTweetBlockFetcher {
  private File file;
  private String output;
  private ConcurrentSkipListMap<Long, String> tweets = new ConcurrentSkipListMap<Long, String>();

  private final AsyncHttpClient asyncHttpClient = new AsyncHttpClient();

  private static String prefix;

  public AsyncTweetBlockFetcher(File file, String output) {
    this.file = file;
    this.output = output;
  }

  public void fetch() throws IOException {
    long start = System.currentTimeMillis();
    System.out.println("Sarting to process " + file);

    int cnt = 0;
    try {
      BufferedReader data = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
      String line;
      while ((line = data.readLine()) != null) {
        String[] arr = line.split("\t");
        long id = Long.parseLong(arr[0]);
        String url = String.format("%s/1/statuses/show/%s.json", prefix, id);
        //System.out.println(url);
        asyncHttpClient.prepareGet(url).execute(new TweetFetcherHandler(id, arr[1]));

        cnt++;

        if ( cnt % 500 == 0 ) {
          System.out.println(cnt + " requests submitted. Sleeping.");

          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }    

    long end = System.currentTimeMillis();
    long duration = end - start;
    System.out.println(cnt + " requests");
    System.out.println(tweets.size() + " tweets fetched in " + duration + "ms");

    System.out.println("Writing tweets...");
    int written = 0;
    FileWriter out = new FileWriter(new File(output));
    for ( Map.Entry<Long, String> entry : tweets.entrySet()) {
      if ( entry.getValue().matches("\\s*") )
        continue;
      written++;
      out.write(entry.getValue() + "\n");
    }
    out.close();
    System.out.println(written + " written.");
    System.out.println("Done!");
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
//        final byte[] b = new byte[10 * 1024];
//        response.get.getResponseBodyAsStream().read(b);
//        String s = new String(b);

        String s = response.getResponseBody();
        tweets.put(id, s);
      } else if (response.getStatusCode() >= 500) {
        System.out.println("Error status " + response.getStatusCode() + ": " + id);
        String url = String.format("%s/1/statuses/show/%s.json", prefix, id);
        System.out.println("Resubmitting: " + url);
        asyncHttpClient.prepareGet(url).execute(new TweetFetcherHandler(id, username));
      }

      return response;
    }

    @Override
    public void onThrowable(Throwable t) {
      System.out.println("Error: " + t);
      String url = String.format("%s/1/statuses/show/%s.json", prefix, id);
      System.out.println("Resubmitting: " + url);
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
    new AsyncTweetBlockFetcher(new File(args[1]), args[2]).fetch();
  }
}
