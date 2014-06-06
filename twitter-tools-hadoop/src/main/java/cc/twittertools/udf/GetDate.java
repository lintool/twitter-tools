package cc.twittertools.udf;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import cc.twittertools.index.LowerCaseEntityPreservingFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

public class GetDate extends EvalFunc<String>{
	
	public String exec(Tuple input) throws IOException {
		if(input == null || input.size() == 0){
			return null;
		}
		//Standard Time Format: Tue Feb 08 23:59:59 +0000 2011
		try{
			String str = (String) input.get(0);
			String[] groups = str.split("\\s+");
			String year = groups[5];
			String month = groups[1];
			String day= groups[2];
			return year+" "+month+" "+day;
		}catch(Exception e){
			throw new IOException("caught exception",e);
		}
	}

}
