package cc.twittertools.search.indexing;

import java.io.IOException;
import java.io.Reader;

//import org.apache.lucene.analysis.LetterTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.Version;

/**
 * A variant of Lucene's LowerCaseTokenizer, but preserves hashtags and mentions.
 */
public class LowerCaseHashtagMentionPreservingTokenizer extends Tokenizer {
  public LowerCaseHashtagMentionPreservingTokenizer(Version matchVersion, Reader in) {
    super(in);
  }

  // as of V.4.*, no longer overrides
  protected boolean isTokenChar(int c) {
    return Character.isLetter(c) || Character.isDigit(c) || c == (int) '#' || c == (int) '@';
  }

  // as of V.4.*, no longer overrides
  protected int normalize(int c) {
    return Character.isLetter(c) ? Character.toLowerCase(c) : c;
  }

  // ???
  @Override
  public boolean incrementToken() throws IOException {
    // TODO Auto-generated method stub
    return false;
  }
}