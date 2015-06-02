package cc.twittertools.download;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.digest.DigestUtils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;

public class CBOROutput extends GZipJSONOutput {

  CBORFactory cbor_fac;
  ObjectMapper cbor_mapper;
  CBORGenerator cbor_gen;
  JsonFactory json_fac;
  ObjectMapper json_mapper;
  OutputStream os;
  
  public CBOROutput(File output_file) {
    super(output_file);
    cbor_fac = new CBORFactory();
    cbor_fac.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
    cbor_mapper = new ObjectMapper(cbor_fac);
    json_fac = new JsonFactory();
    json_mapper = new ObjectMapper(json_fac);
  }

  @Override
  public void open() throws Exception {
    os = new GZIPOutputStream(new FileOutputStream(output_file));
    cbor_gen = cbor_fac.createGenerator(os);
  }

  @Override @SuppressWarnings("unchecked")
  public void write(String html) throws Exception {
    ObjectNode json_tweet = ext.extractTweet(html);
    ObjectNode cca_obj = wrap_cca(html, json_tweet);
    cbor_mapper.writeTree(cbor_gen, cca_obj);
  }
  
  @Override
  public void close() throws Exception {
    os.close();
  }
  
  @SuppressWarnings("unchecked")
  public ObjectNode wrap_cca(String tweet, ObjectNode json) throws JsonProcessingException, IOException {
    final JsonNodeFactory jf = JsonNodeFactory.instance;
    
    ObjectNode cca_obj = jf.objectNode();
    long id = json.get("id").asLong();
    String username = json.get("user").get("screen_name").textValue();
    String url = AsyncHTMLStatusBlockCrawler.getUrl(id, username);
    cca_obj.put("url", url);
    cca_obj.put("timestamp", System.currentTimeMillis());
    cca_obj.putNull("request");

    ObjectNode response = cca_obj.putObject("response");
    response.put("status", "200");
    response.put("server", "twitter.com");
    response.putArray("headers").add(jf.arrayNode().add("Content-Type").add("text/html"));
    response.put("body", tweet);

    cca_obj.put("key", "com_twitter_" + DigestUtils.sha1Hex(url));
    
    ArrayNode indices = cca_obj.arrayNode();
    indices.addObject().put("crawl", "tweets");
    cca_obj.set("indices", indices);
    
    ArrayNode features = cca_obj.arrayNode();
    features.addObject().replace("json", json);
    cca_obj.set("features", features);
    return cca_obj;
  }
}
