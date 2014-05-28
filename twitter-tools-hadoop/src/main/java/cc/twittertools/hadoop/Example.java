package cc.twittertools.hadoop;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import cc.twittertools.index.LowerCaseEntityPreservingFilter;

public class Example {
	
	public static void main(String[] args) throws IOException{
		String o = "I have a dream@lijia http://www.google.com/trends?pm=2";
		Tokenizer source = new WhitespaceTokenizer(Version.LUCENE_43, new StringReader((String)o));
        TokenStream tokenstream = new LowerCaseEntityPreservingFilter(source);
        tokenstream.reset();
        while (tokenstream.incrementToken()){
        	String token = tokenstream.getAttribute(CharTermAttribute.class).toString();
        	System.out.println(token);
        }
	}
}
