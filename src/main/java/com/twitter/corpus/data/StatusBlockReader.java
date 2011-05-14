package com.twitter.corpus.data;

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
public class StatusBlockReader implements StatusStream {
  private final BufferedReader br;

  public StatusBlockReader(File file) throws IOException {
    if (!file.getName().endsWith(".gz")) {
      throw new IOException("Expecting .gz compressed file!");
    }

    br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)), "UTF-8"));
  }

  /**
   * Returns the next status, or <code>null</code> if no more statuses.
   */
  public Status next() throws IOException {
    String raw = br.readLine();

    // Check to see if we've reached end of file.
    if ( raw == null) {
      return null;
    }

    return Status.fromJson(raw);
  }

  public void close() throws IOException {
    br.close();
  }
}
