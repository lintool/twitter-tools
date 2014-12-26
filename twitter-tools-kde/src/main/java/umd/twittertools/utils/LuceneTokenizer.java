package umd.twittertools.utils;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import cc.twittertools.index.LowerCaseEntityPreservingFilter;

public class LuceneTokenizer {
	
	public static List<String> tokenize(TokenStream tokenstream) {
    List<String> output = new ArrayList<String>();
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
