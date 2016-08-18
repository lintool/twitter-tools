package cc.twittertools.download;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import com.fasterxml.jackson.databind.node.ObjectNode;

import cc.twittertools.corpus.data.HTMLStatusExtractor;

public class GZipJSONOutput implements CrawlerOutputWriter {

  protected File output_file;
  protected OutputStreamWriter out;
  protected HTMLStatusExtractor ext;
  
  public GZipJSONOutput(File output_file) {
    this.output_file = output_file;
    ext = new HTMLStatusExtractor();
  }

  @Override
  public void open() throws Exception {
    out = new OutputStreamWriter(new GZIPOutputStream(
        new FileOutputStream(output_file)), "UTF-8");
  }

  @Override
  public void write(String s) throws Exception {
    ObjectNode json = ext.extractTweet(s);
    out.write(json.toString() + "\n");
  }

  @Override
  public void close() throws Exception {
    out.close();
  }

}
