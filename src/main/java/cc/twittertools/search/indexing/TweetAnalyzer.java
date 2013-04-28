package cc.twittertools.search.indexing;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.util.Version;

public class TweetAnalyzer extends Analyzer {
  private final Version matchVersion;

  public TweetAnalyzer(Version matchVersion) {
    this.matchVersion = matchVersion;
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
    Tokenizer source = new WhitespaceTokenizer(matchVersion, reader);
    TokenStream filter = new LowerCaseEntityPreservingFilter(source);
    // Porter stemmer ignores words which are marked as keywords
    filter = new PorterStemFilter(filter);
    return new TokenStreamComponents(source, filter);
  }
  
}