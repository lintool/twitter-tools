package cc.twittertools.corpus.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;


import org.apache.commons.lang.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Object representing a status.
 */
public class Status {
  private static final JsonParser parser = new JsonParser();

  private long id;
  private String screenname;
  private String createdAt;
  private String text;
  private JsonObject jsonObject;
  private String jsonString;

  protected Status() {}
  
  public long getId() {
    return id;
  }

  public String getText() {
    return text;
  }

  public String getCreatedAt() {
    return createdAt;
  }

  public String getScreenname() {
    return screenname;
  }

  public JsonObject getJsonObject() {
    return jsonObject;
  }

  public String getJsonString() {
    return jsonString;
  }

  public static Status fromJson(String json) {
    Preconditions.checkNotNull(json);

    JsonObject obj = (JsonObject) parser.parse(json);
    if (obj.get("html") == null)
      return null;

    Status status = new Status();
    String html = obj.get("html").getAsString();
    html = StringEscapeUtils.unescapeXml(html);
    
    // use some jsoup magic to parse html and fetch require elements
    org.jsoup.nodes.Document document = Jsoup.parse(html);

    Element dateElement = document.select("a").last();
    status.createdAt = dateElement.text();

    Element textElement = document.select("p").first();
    status.text = textElement.text();

    String idRaw = parseUrlGetLastElementInPath(obj.get("url").getAsString());
    status.id = Long.parseLong(idRaw);

    status.screenname = parseUrlGetLastElementInPath(obj.get("author_url").getAsString());

    // TODO: We need to parse out the other fields.

    status.jsonObject = obj;
    status.jsonString = json;

    return status;
  }

  private static String parseUrlGetLastElementInPath(String string) {
    String[] split = string.split("/");
    String idRaw = split[split.length-1];
    return idRaw;
  }
}
