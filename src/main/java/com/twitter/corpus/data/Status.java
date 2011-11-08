package com.twitter.corpus.data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Object representing a status.
 */
public class Status {
  private static final HtmlStatusExtractor extractor = new HtmlStatusExtractor();
  private static final JsonParser parser = new JsonParser();

  private long id;
  private String screenname;
  private String createdAt;
  private long timestamp;
  private String text;
  private int httpStatusCode;
  private JsonObject jsonObject;
  private String jsonString;
  private String replyOf;
  private String location;
  private String placeId;
  private String latitude;
  private String longitude;

  private static SimpleDateFormat dateFormat = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
  
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
  
  public long getTimestamp() {
    return timestamp;
  }

  public String getScreenname() {
    return screenname;
  }

  public int getHttpStatusCode() {
    return httpStatusCode;
  }

  public JsonObject getJsonObject() {
    return jsonObject;
  }

  public String getJsonString() {
    return jsonString;
  }
  
  public String getReplyOf() {
    return replyOf;
  }
  
  public String getLatitude() {
    return latitude;
  }
  
  public String getLongitude() {
    return longitude;
  }

  public String getLocation() {
    return location;
  }
  
  public String getPlaceId() {
    return placeId;
  }
  
  public static Status fromJson(String json) {
    Preconditions.checkNotNull(json);

    JsonObject obj = (JsonObject) parser.parse(json);

    Status status = new Status();
    status.text = obj.get("text").getAsString();
    status.id = obj.get("id").getAsLong();
    status.screenname = obj.get("user").getAsJsonObject().get("screen_name").getAsString();
    status.createdAt = obj.get("created_at").getAsString();

    // TODO: We need to parse out the other fields.

    status.jsonObject = obj;
    status.jsonString = json;

    return status;
  }

  public static Status fromHtml(long id, String username, int httpStatus, String html) {
    Preconditions.checkNotNull(html);
    Preconditions.checkNotNull(username);

    Status status = new Status();

    status.id = id;
    status.screenname = username;
    status.httpStatusCode = httpStatus;
    status.text = extractor.extractTweet(html);
    status.createdAt = extractor.extractTimestamp(html);
    
    if ( status.createdAt != null) {
      try {
        Date tmpdate;
        tmpdate = dateFormat.parse(status.createdAt);
        status.timestamp = tmpdate.getTime()/1000; // division by 1000 delete milliseconds of getTime()
      } catch (ParseException e) {
        status.timestamp = 0;
      } 
    }
    
    
    status.replyOf = extractor.extractReplyOf(html);
    status.location = extractor.extractLocation(html);
    status.placeId = extractor.extractPlaceId(html);
    
    String latlng = extractor.extractLatLng(html);
    if (latlng != null) {
      String [] tmp = latlng.split(",", 2);
      if (tmp.length == 2) {
        status.latitude = tmp[0];
        status.longitude = tmp[1];
      }
    }

    // TODO: Note that http status code 302 indicates a redirect, which indicates a retweet. I.e.,
    // the status redirects to the original retweeted status. Note that this is not currently
    // handled.

    return status;
  }
}
