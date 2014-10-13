package cc.twittertools.index;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

public class LuceneTokenizer {
  
  public static List<String> tokenize(String input) {
    List<String> output = new ArrayList<String>();
    Tokenizer source = new WhitespaceTokenizer(Version.LUCENE_43, new StringReader((String)input));
    TokenStream tokenstream = new LowerCaseEntityPreservingFilter(source);
    try {
      tokenstream.reset();
      while (tokenstream.incrementToken()){
        String token = tokenstream.getAttribute(CharTermAttribute.class).toString();
        if (token.length() > 0) {
          output.add(token);
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return output;
  }
  
}
