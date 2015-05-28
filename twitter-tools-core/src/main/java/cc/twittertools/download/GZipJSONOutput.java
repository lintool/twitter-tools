package cc.twittertools.download;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

public class GZipJSONOutput implements CrawlerOutputWriter {

  protected File output_file;
  protected OutputStreamWriter out;
  
  public GZipJSONOutput(File output_file) {
    this.output_file = output_file;
  }

  @Override
  public void open() throws Exception {
    out = new OutputStreamWriter(new GZIPOutputStream(
        new FileOutputStream(output_file)), "UTF-8");
  }

  @Override
  public void write(String s) throws Exception {
    out.write(s + "\n");
  }

  @Override
  public void close() throws Exception {
    out.close();
  }

}
