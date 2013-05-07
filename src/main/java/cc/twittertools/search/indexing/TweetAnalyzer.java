package cc.twittertools.search.indexing;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;

import com.google.common.base.Preconditions;

public class TweetAnalyzer extends Analyzer {
  private final Version matchVersion;

  public TweetAnalyzer(Version matchVersion) {
    this.matchVersion = Preconditions.checkNotNull(matchVersion);
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
    return new TokenStreamComponents(
        new LowerCaseHashtagMentionPreservingTokenizer(matchVersion, reader));
  }
}