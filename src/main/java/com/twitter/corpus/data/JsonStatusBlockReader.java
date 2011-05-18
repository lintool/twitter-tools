package com.twitter.corpus.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import com.google.common.base.Preconditions;

/**
 * Abstraction for an stream of statuses, backed by an underlying gzipped file with JSON-encoded
 * tweets, one per line.
 */
public class JsonStatusBlockReader implements StatusStream {
  private final BufferedReader br;

  public JsonStatusBlockReader(File file) throws IOException {
    Preconditions.checkNotNull(file);

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
