package cc.twittertools.index;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.util.Version;

import com.google.common.base.Preconditions;

public final class TweetAnalyzer extends Analyzer {
  private final Version matchVersion;
  private final boolean stemming;

  public TweetAnalyzer(Version matchVersion, boolean stemming) {
    this.matchVersion = Preconditions.checkNotNull(matchVersion);
    this.stemming = stemming;
  }

  public TweetAnalyzer(Version matchVersion) {
    this(matchVersion, true);
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
    Tokenizer source = new WhitespaceTokenizer(matchVersion, reader);
    TokenStream filter = new LowerCaseEntityPreservingFilter(source);

    if (stemming) {
      // Porter stemmer ignores words which are marked as keywords
      filter = new PorterStemFilter(filter);
    }
    return new TokenStreamComponents(source, filter);
  }

}