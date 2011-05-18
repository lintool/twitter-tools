package com.twitter.corpus.data;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Abstraction for a corpus of statuses. A corpus is assumed to consist of a number of blocks, each
 * represented by SequenceFile, under a single directory. This object allows the caller to read
 * through all blocks, in sorted lexicographic order of the files.
 */
public class StatusCorpusReader implements StatusStream {
  private final FileStatus[] files;
  private final FileSystem fs;

  private int nextFile = 0;
  private StatusBlockReader currentBlock = null;

  public StatusCorpusReader(Path directory, FileSystem fs) throws IOException {
    this.fs = fs;
    if (!fs.getFileStatus(directory).isDir()) {
      throw new IOException("Expecting " + directory + " to be a directory!");
    }

    files = fs.listStatus(directory);

    if (files.length == 0) {
      throw new IOException(directory + " does not contain any files!");
    }
  }

  /**
   * Returns the next status, or <code>null</code> if no more statuses.
   */
  public Status next() throws IOException {
    if (currentBlock == null) {
      currentBlock = new StatusBlockReader(files[nextFile].getPath(), fs);
      nextFile++;
    }

    Status status = null;
    while (true) {
      status = currentBlock.next();
      if (status != null) {
        return status;
      }

      if (nextFile >= files.length) {
        // We're out of files to read. Must be the end of the corpus.
        return null;
      }

      currentBlock.close();
      // Move to next file.
      currentBlock = new StatusBlockReader(files[nextFile].getPath(), fs);
      nextFile++;
    }
  }

  public void close() throws IOException {
    currentBlock.close();
  }
}
