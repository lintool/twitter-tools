package cc.twittertools.download;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;

public class CBOROutput extends GZipJSONOutput {

  CBORFactory cbor_fac;
  ObjectMapper cbor_mapper;
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
  }

  @Override @SuppressWarnings("unchecked")
  public void write(String s) throws Exception {
    Map<String, Object> obj;
    try {
      obj = json_mapper.readValue(s, Map.class);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    cbor_mapper.writeValue(os, obj);
  }
  
  @Override
  public void close() throws Exception {
    os.close();
  }
}
