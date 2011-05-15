package com.twitter.corpus.download;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Future;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.twitter.corpus.data.Status;

public class FetchStatusTest {
  private String getUrl(String username, long id) throws Exception {
    return String.format("http://twitter.com/statuses/show/%d.json", id);
  }

  @Test
  public void basicFamous() throws Exception {
    String url = getUrl("jkrums", 1121915133L);
    AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
    AsyncHttpClient.BoundRequestBuilder request = asyncHttpClient.prepareGet(url);
    Future<Response> f = request.execute();
    Response response = f.get();

    // Make sure status is OK.
    assertEquals(200, response.getStatusCode());
    String s = response.getResponseBody();

    Status status = Status.fromJson(s);
    assertEquals(1121915133L, status.getId());
    assertEquals("jkrums", status.getScreenname());
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(FetchStatusTest.class);
  }
}
