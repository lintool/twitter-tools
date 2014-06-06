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
		//Test GetInterval Correctness
		try{
			String str = "Tue Oct 01 00:07:43 +0000 2011";
			String[] groups = str.split("\\s+");
			String time = groups[3];
			String[] timeGroups= time.split(":");
			int interval = (Integer.valueOf(timeGroups[0]))*12 + (Integer.valueOf(timeGroups[1])/5) + 1;
			System.out.println(interval);
		}catch(Exception e){
			throw new IOException("caught exception",e);
		}
	}
}
