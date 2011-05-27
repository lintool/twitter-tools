package com.twitter.corpus.demo;

import java.io.Reader;

import org.apache.lucene.analysis.ReusableAnalyzerBase;
import org.apache.lucene.util.Version;

public class TweetAnalyzer extends ReusableAnalyzerBase {
  private Version matchVersion;

  /**
   * Creates a new {@link TweetAnalyzer}.
   */
  public TweetAnalyzer(Version matchVersion) {
    this.matchVersion = matchVersion;
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
    return new TokenStreamComponents(new LowerCaseHashtagMentionPreservingTokenizer(matchVersion, reader));
  }
}