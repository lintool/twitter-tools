package cc.twittertools.download;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Future;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import cc.twittertools.corpus.data.Status;
import cc.twittertools.download.AsyncEmbeddedJsonStatusBlockCrawler;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

public class FetchStatusTest {

  @Test
  public void basicFamous() throws Exception {
    String url = AsyncEmbeddedJsonStatusBlockCrawler.getUrl(1121915133L, "jkrums");
    AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
    AsyncHttpClient.BoundRequestBuilder request = asyncHttpClient.prepareGet(url);
    Future<Response> f = request.execute();
    Response response = f.get();

    // Make sure status is OK.
    assertEquals(200, response.getStatusCode());

    String html = response.getResponseBody("UTF-8");
    Status status = Status.fromHtml(html);
    assertEquals(1121915133L, status.getId());
    assertEquals("jkrums", status.getScreenname());
    assertEquals("http://twitpic.com/135xa - There's a plane in the Hudson. I'm on the ferry going to pick up the people. Crazy.", status.getText());

    asyncHttpClient.close();
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(FetchStatusTest.class);
  }
}
