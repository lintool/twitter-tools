package cc.twittertools.udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import cc.twittertools.index.TweetAnalyzer;

public class Tokenize extends EvalFunc<String>{
  
  public String exec(Tuple tuple) throws IOException {
    if(tuple == null || tuple.size() == 0){
      return null;
    }
    String output = "";
    String input = (String) tuple.get(0);
    if (input == null || input.length() == 0) {
      return null;
    }
    TweetAnalyzer tokenizer = new TweetAnalyzer(Version.LUCENE_43, false);
    TokenStream tokenstream = tokenizer.tokenStream("text", input);
    tokenstream.reset();
    while (tokenstream.incrementToken()){
      String token = tokenstream.getAttribute(CharTermAttribute.class).toString();
      if (token.length() > 0) {
        output += token + " ";
      }
    }
    return output;
  }
}
