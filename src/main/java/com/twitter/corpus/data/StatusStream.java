package com.twitter.corpus.data;

import java.io.IOException;

/**
 * Abstraction for a stream of statuses. Ordering of the statuses is left to the implementation.
 */
public interface StatusStream {
  public Status next() throws IOException;
  public void close() throws IOException;
}
