package cc.twittertools.corpus.data;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;


/**
 * Abstraction for a corpus of statuses. A corpus is assumed to consist of a number of blocks, each
 * represented by a gzipped file within a root directory. This object will allow to caller to read
 * through all blocks, in sorted lexicographic order of the files.
 */
public class TSVStatusCorpusReader implements StatusStream {
  private final File[] files;
  private int nextFile = 0;
  private TSVStatusBlockReader currentBlock = null;

  public TSVStatusCorpusReader(File file) throws IOException {

    if (!file.isDirectory()) {
      throw new IOException("Expecting " + file + " to be a directory!");
    }

    files = file.listFiles(new FileFilter() {
      public boolean accept(File path) {
        return path.getName().endsWith(".gz") ? true : false;
      }
    });

    if (files.length == 0) {
      throw new IOException(file + " does not contain any .gz files!");
    }
  }

  /**
   * Returns the next status, or <code>null</code> if no more statuses.
   */
  public Status next() throws IOException {
    if (currentBlock == null) {
      currentBlock = new TSVStatusBlockReader(files[nextFile]);
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
      currentBlock = new TSVStatusBlockReader(files[nextFile]);
      nextFile++;
    }
  }

  public void close() throws IOException {
    currentBlock.close();
  }
}
