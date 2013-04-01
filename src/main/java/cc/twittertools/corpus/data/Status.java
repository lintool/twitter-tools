package cc.twittertools.corpus.data;

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
  
  public static Status fromTSV(String tsv) {
	  String[] columns = tsv.split("\t");
	  
	  if(columns.length < 4) {
		  System.err.println("error parsing: " + tsv);
		  System.exit(-1);
	  }
	  
	  Status status = new Status();
	  status.id = Long.parseLong(columns[0]);
	  status.screenname = columns[1];
	  status.createdAt = columns[2];
	  
	  StringBuilder b = new StringBuilder();
	  for(int i=3; i<columns.length; i++) {
		  b.append(columns[i] + " ");
	  }
	  status.text = b.toString().trim();
	  
	  return status;
  }
}
