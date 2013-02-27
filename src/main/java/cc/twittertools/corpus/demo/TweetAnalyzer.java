package cc.twittertools.corpus.demo;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;

import org.apache.lucene.util.Version;

public class TweetAnalyzer extends Analyzer {
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
    //return new TokenStreamComponents(new Tokenizer(reader));
  }
}