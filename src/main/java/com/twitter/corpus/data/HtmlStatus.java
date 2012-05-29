package com.twitter.corpus.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

public class HtmlStatus implements Writable {
  // We want to keep track of version to future-proof.
  private static final byte VERSION = 1;

  private byte version;
  private int httpStatusCode;
  private long timestamp;
  private String html;

  private static final int CHUNK_SIZE = 16 * 1024; // use 16K chunks

  public HtmlStatus() {
    this.version = VERSION;
  }

  public HtmlStatus(int httpStatusCode, long timestamp, String html) {
    this.version = VERSION;
    this.httpStatusCode = httpStatusCode;
    this.timestamp = timestamp;
    this.html = Preconditions.checkNotNull(html);
  }

  public int getHttpStatusCode() {
    return httpStatusCode;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getHtml() {
    return html;
  }

  /**
   * Deserializes the object.
   */
  public void readFields(DataInput in) throws IOException {
    this.version = in.readByte();
    this.httpStatusCode = in.readInt();
    this.timestamp = in.readLong();
    this.html = in.readUTF();
    // handle new serialized format
    String newIndicator = "BEGIN " + this.timestamp + " ";
    if (html.startsWith(newIndicator)) {
      int numChunks = Integer.parseInt(html.substring(newIndicator.length()));
      this.html = "";
      for (int i = 0; i < numChunks; i++) {
        this.html += in.readUTF();
      }
    }
  }

  /**
   * Serializes this object.
   */
  public void write(DataOutput out) throws IOException {
    out.writeByte(version);
    out.writeInt(httpStatusCode);
    out.writeLong(timestamp);
    // UTF allows blocks of at most 64K, so divide html into chunks while
    // maintaining backward compatibility using a new header
    int numChunks = (int)Math.ceil((double)html.length() / (double)CHUNK_SIZE);
    String header = "BEGIN " + timestamp + " " + numChunks;
    out.writeUTF(header);
    for (int i = 0; i < numChunks - 1; i++) {
        out.writeUTF(html.substring(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE));
    }
    out.writeUTF(html.substring((numChunks - 1) * CHUNK_SIZE));
  }

  @Override
  public HtmlStatus clone() {
    return new HtmlStatus(httpStatusCode, timestamp, html);
  }

  @Override
  public String toString() {
    return String.format("[Fetched at %d with status %d:\n%s]\n", timestamp, httpStatusCode, html);
  }
}
