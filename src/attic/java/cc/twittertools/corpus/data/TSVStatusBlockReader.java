package cc.twittertools.corpus.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;


/**
 * Abstraction for an stream of statuses, backed by an underlying gzipped file with JSON-encoded
 * tweets, one per line.
 */
public class TSVStatusBlockReader implements StatusStream {
  private final BufferedReader br;

  public TSVStatusBlockReader(File file) throws IOException {

    if (!file.getName().endsWith(".gz")) {
      throw new IOException("Expecting .gz compressed file!");
    }

    br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)), "UTF-8"));
  }

  /**
   * Returns the next status, or <code>null</code> if no more statuses.
   */
  public Status next() throws IOException {
    Status nxt = null;
    String raw = null;

    while (nxt == null) {
	raw = br.readLine();

	// Check to see if we've reached end of file.
	if ( raw == null) {
	    return null;
	}

	nxt = Status.fromTSV(raw);
    }
    return Status.fromTSV(raw);
  }

  public void close() throws IOException {
    br.close();
  }
}
