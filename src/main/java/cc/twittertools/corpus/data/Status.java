package cc.twittertools.corpus.data;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Object representing a status.
 */
public class Status {
  private static final JsonParser JSON_PARSER = new JsonParser();
  private static final String DATE_FORMAT = "EEE MMM d k:m:s ZZZZZ yyyy"; //"Fri Mar 29 11:03:41 +0000 2013"; 
  private long id;
  private String screenname;
  private String createdAt;
  private long epoch;
  private String text;
  private JsonObject jsonObject;
  private String jsonString;
  private String lang;
  private long inReplyToStatusId;
  private long inReplyToUserId;
  private int followersCount;
  private int friendsCount;
  private int statusesCount;

  protected Status() {}

  public long getId() {
    return id;
  }

  public String getScreenname() {
    return screenname;
  }


  public String getCreatedAt() {
    return createdAt;
  }

  public long getEpoch() {
    return epoch;
  }

  public String getText() {
    return text;
  }

  public JsonObject getJsonObject() {
    return jsonObject;
  }

  public String getJsonString() {
    return jsonString;
  }

  public String getLang() {
    return lang;
  }

  public int getFollowersCount() {
    return followersCount;
  }

  public int getFriendsCount() {
    return friendsCount;
  }

  public int getStatusesCount() {
    return statusesCount;
  }
  
  public long getInReplyToStatusId() {
    return inReplyToStatusId;
  }

  public long getInReplyToUserId() {
    return inReplyToUserId;
  }
  
  public static Status fromJson(String json) {
    JsonObject obj = null;
    try {
      obj = (JsonObject) JSON_PARSER.parse(json);
    } catch (Exception e) {
      e.printStackTrace();
      // Catch any malformed JSON.
      return null;
    }

    if (obj.get("text") == null) {
      return null;
    }

    Status status = new Status();
    status.text = obj.get("text").getAsString();
    status.id = obj.get("id").getAsLong();
    status.screenname = obj.get("user").getAsJsonObject().get("screen_name").getAsString();
    status.createdAt = obj.get("created_at").getAsString();

    try {
      status.epoch = (new SimpleDateFormat(DATE_FORMAT)).parse(status.createdAt).getTime() / 1000;
    } catch (ParseException e) {
      status.epoch = -1L;
    }

    try {
      status.inReplyToStatusId = obj.get("in_reply_to_status_id").getAsLong();
    } catch (Exception e) {
      status.inReplyToStatusId = -1L;
    }
    
    try {
      status.inReplyToUserId = obj.get("in_reply_to_user_id").getAsLong();
    } catch (Exception e) {
      status.inReplyToUserId = -1L;
    }
    
    //status.lang = obj.get("user").getAsJsonObject().get("lang").getAsString();
    try {
      status.lang = obj.get("lang").getAsString();
    } catch (Exception e) {
      status.lang = "unknown";
    }
    
    status.followersCount = obj.get("user").getAsJsonObject().get("followers_count").getAsInt();
    status.friendsCount = obj.get("user").getAsJsonObject().get("friends_count").getAsInt();
    status.statusesCount = obj.get("user").getAsJsonObject().get("statuses_count").getAsInt();


    status.jsonObject = obj;
    status.jsonString = json;

    return status;
  }

  public static Status fromTSV(String tsv) {
    String[] columns = tsv.split("\t");

    if (columns.length < 4) {
      System.err.println("error parsing: " + tsv);
      return null;
    }

    Status status = new Status();
    status.id = Long.parseLong(columns[0]);
    status.screenname = columns[1];
    status.createdAt = columns[2];

    StringBuilder b = new StringBuilder();
    for (int i = 3; i < columns.length; i++) {
      b.append(columns[i] + " ");
    }
    status.text = b.toString().trim();

    return status;
  }
}
