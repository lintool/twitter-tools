package com.twitter.corpus.demo;

import java.io.Reader;

import org.apache.lucene.analysis.LetterTokenizer;
import org.apache.lucene.util.Version;

/**
 * A variant of Lucene's LowerCaseTokenizer, but preserves hashtags and mentions.
 */
public class LowerCaseHashtagMentionPreservingTokenizer extends LetterTokenizer {
  public LowerCaseHashtagMentionPreservingTokenizer(Version matchVersion, Reader in) {
    super(matchVersion, in);
  }

  @Override
  protected boolean isTokenChar(int c) {
    return Character.isLetter(c) || Character.isDigit(c) || c == (int) '#' || c == (int) '@';
  }

  @Override
  protected int normalize(int c) {
    return Character.isLetter(c) ? Character.toLowerCase(c) : c;
  }
}