package com.twitter.corpus.data;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import edu.umd.cloud9.io.pair.PairOfLongString;

/**
 * Abstraction for a stream of statuses, backed by an underlying SequenceFile.
 */
public class StatusBlockReader implements StatusStream {
  private final SequenceFile.Reader reader;

  private final PairOfLongString key = new PairOfLongString();
  private final StatusHtml value = new StatusHtml();

  public StatusBlockReader(Path path, FileSystem fs) throws IOException {
    reader = new SequenceFile.Reader(fs, path, fs.getConf());
  }

  /**
   * Returns the next status, or <code>null</code> if no more statuses.
   */
  public Status next() throws IOException {
    if (!reader.next(key, value))
      return null;

    return Status.fromHtml(key.getLeftElement(), key.getRightElement(),
        value.getHttpStatusCode(), value.getHtml());
  }

  public void close() throws IOException {
    reader.close();
  }
}
