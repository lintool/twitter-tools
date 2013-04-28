package cc.twittertools.search.indexing;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

import com.twitter.Regex;

public class LowerCaseEntityPreservingFilter extends TokenFilter {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);
  private char[] tailBuffer = null;

  public LowerCaseEntityPreservingFilter(TokenStream in) {
    super(in);
  }

  @Override
  public boolean incrementToken() throws IOException {
    // There is no saved state, and nothing remains on the input buffer
    if (tailBuffer == null && !input.incrementToken()) {
      return false;
    }

    final char[] buffer = termAtt.buffer();

    // Reload any saved sate
    if (tailBuffer != null) {
      System.arraycopy(tailBuffer, 0, buffer, 0, tailBuffer.length);
      termAtt.setLength(tailBuffer.length);
      tailBuffer = null;
    }

    // Deal with any entities
    if (isEntity(termAtt.toString())) {
      if (Regex.VALID_URL.matcher(termAtt.toString()).matches()) {
        keywordAttr.setKeyword(true);
        return true; // Don't touch URLs
      }

      // Convert the entity to lowercase
      for (int i = 0; i < termAtt.length(); i++) {
        buffer[i] = Character.toLowerCase(buffer[i]);
      }

      // At this stage, if it's a valid entity and doesn't have any
      // proceeding characters, then we can stop processing
      if (isEntityDelimiter(0)) {
        keywordAttr.setKeyword(true);
        return true;
      }

      // Cases where there are characters before the entity sign.
      // Split them off and process them separately
      for (int i = 0; i < termAtt.length(); i++) {
        buffer[i] = Character.toLowerCase(buffer[i]);
        if (isEntityDelimiter(i)) {
          tailBuffer = Arrays.copyOfRange(buffer, i, termAtt.length());
          termAtt.setLength(i);
          break;
        }
      }

    } else {

      // It's not a URL, lowercase it
      for (int i = 0; i < termAtt.length(); i++) {
        buffer[i] = Character.toLowerCase(buffer[i]);
      }

      // Check for non-whitespace, non-entity (@, #, _) delimiters in the
      // term
      for (int i = 0; i < termAtt.length(); i++) {
        if (isNonentityDelimiter(i)) {
          // Remove the tail of the string from the buffer and save it
          // for the next iteration
          tailBuffer = Arrays.copyOfRange(buffer, i + 1, termAtt.length());
          termAtt.setLength(i);
          break;
        }
      }
      
      // TODO: Preserve Email Addresses

      if (isEntity(termAtt.toString())) {
        // This was an entity with some trailing text - we've removed
        // the tail, all that remains is the entity
        keywordAttr.setKeyword(true);
        return true;
      }

      // It wasn't an entity - we can use the entity markers as delimiters
      for (int i = 0; i < termAtt.length(); i++) {
        if (isEntityDelimiter(i)) {
          // Remove the tail of the string from the buffer and save it
          // for the next iteration
          tailBuffer = Arrays.copyOfRange(buffer, i + 1, termAtt.length());
          termAtt.setLength(i);
          break;
        }
      }

    }

    removeNonAlphanumeric();
    return true;
  }

  /**
   * Remove all non-alphanumeric characters from the buffer
   */
  public void removeNonAlphanumeric() {
    final char[] buffer = termAtt.buffer();
    // Remove any remaining non-alphanumeric characters
    for (int i = 0; i < termAtt.length(); i++) {
      if (!(Character.isAlphabetic(buffer[i]) || Character.isDigit(buffer[i]))) {
        System.arraycopy(buffer, i + 1, buffer, i, buffer.length - 1 - i);
        termAtt.setLength(termAtt.length() - 1);
        i--; // Correct for the (now displaced) buffer position
      }
    }
  }

  /**
   * Check if the given string is a valid entity (mention, hashtag or URL)
   */
  public boolean isEntity(String term) {
    return Regex.VALID_URL.matcher(term).matches()
        || Regex.VALID_MENTION_OR_LIST.matcher(term).matches()
        || Regex.VALID_HASHTAG.matcher(term).matches();
  }

  /**
   * Check if the character at position i in the buffer is a delimiter which
   * wouldn't be used as part of an entity
   */
  public boolean isNonentityDelimiter(int i) {
    final char[] buffer = termAtt.buffer();
    final int bufferLength = termAtt.length();
    switch (buffer[i]) {
    case '-':
    case '?':
    case '!':
    case ',':
    case ';':
    case ':':
    case '(':
    case ')':
    case '[':
    case ']':
    case '/':
    case '\\':
      return true;
    case '.':
      // A complex looking way of saying that a period isn't a delimiter if the
      // characters at current_position +/- 2 are also periods.
      return (i >= 2 && buffer[i - 2] != '.') || ((i + 2) < bufferLength && buffer[i + 2] != '.');
    }
    return false;
  }

  /**
   * Check if the character at position i in the buffer is a delimiter which
   * could be used as party of an entity
   */
  public boolean isEntityDelimiter(int i) {
    final char[] buffer = termAtt.buffer();
    switch (buffer[i]) {
    case '@':
    case '\uFF20': // Unicode @
    case '#':
    case '\uFF03': // Unicode #
    case '_':
      return true;
    }
    return false;
  }

  /**
   * Check if the character at position i in the buffer is a delimiter
   */
  public boolean isDelimiter(int i) {
    return isNonentityDelimiter(i) || isEntityDelimiter(i);
  }
}