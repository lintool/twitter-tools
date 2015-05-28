package cc.twittertools.download;

public interface CrawlerOutputWriter {

  void open() throws Exception;
  void write(String s) throws Exception;
  void close() throws Exception;
}
