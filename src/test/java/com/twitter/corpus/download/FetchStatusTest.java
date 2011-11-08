package com.twitter.corpus.download;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Future;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.twitter.corpus.data.Status;

public class FetchStatusTest {
  @Test
  public void basicFamous() throws Exception {
    String url = AsyncHtmlStatusBlockCrawler.getUrl(1121915133L, "jkrums");
    AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
    AsyncHttpClient.BoundRequestBuilder request = asyncHttpClient.prepareGet(url);
    Future<Response> f = request.execute();
    Response response = f.get();

    // Make sure status is OK.
    assertEquals(200, response.getStatusCode());
    String html = response.getResponseBody("UTF-8");

    Status status = Status.fromHtml(1121915133L, "jkrums", 200, html);
    assertEquals(1121915133L, status.getId());
    assertEquals("jkrums", status.getScreenname());
    assertEquals("http://twitpic.com/135xa - There's a plane in the Hudson. I'm on the ferry going to pick up the people. Crazy.", status.getText());

    asyncHttpClient.close();
  }
  
  @Test
  public void anotherFamous() throws Exception {
    String url = AsyncHtmlStatusBlockCrawler.getUrl(132336185812070401L, "bs");
    AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
    AsyncHttpClient.BoundRequestBuilder request = asyncHttpClient.prepareGet(url);
    Future<Response> f = request.execute();
    Response response = f.get();

    // Make sure status is OK.
    assertEquals(200, response.getStatusCode());
    String html = response.getResponseBody("UTF-8");

    Status status = Status.fromHtml(132336185812070401L,"bs", 200, html);
    assertEquals(132336185812070401L, status.getId());
    assertEquals("bs", status.getScreenname());
    assertEquals("@olofster, wish you were here!", status.getText());
    assertEquals("132332780720898049", status.getReplyOf());
    assertEquals("Centro, Tabasco", status.getLocation());
    assertEquals("a0536fb1dd01b8d5", status.getPlaceId());
    assertEquals("17.97813796", status.getLatitude());
    assertEquals("-92.93089606", status.getLongitude());
    assertEquals(1320386380, status.getTimestamp());
    asyncHttpClient.close();
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(FetchStatusTest.class);
  }
}
