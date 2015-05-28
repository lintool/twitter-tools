package cc.twittertools.download;

import java.io.File;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;

public class CBOROutput extends GZipJSONOutput {

  CBORFactory fac;
  ObjectMapper mapper;
  
  public CBOROutput(File output_file) {
    super(output_file);
    fac = new CBORFactory();
    mapper = new ObjectMapper(fac);
  }

  @SuppressWarnings("unchecked")
  public void write(String s) throws Exception {
    Map<String, Object> obj;
    try {
      obj = mapper.readValue(s, Map.class);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    mapper.writeValue(out, obj);
  }
  
}
