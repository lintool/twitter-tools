package cc.twittertools.corpus.data;

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
    if (obj.get("text") == null)
	return null;

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
}
