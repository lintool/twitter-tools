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

public class TimeDim extends EvalFunc<String>{
	
	static String NOT_FOUND = "-1";
	
	public String exec(Tuple input) throws IOException {
		if(input == null || input.size() == 0){
			return null;
		}
		try{
			String str = (String) input.get(0);
			Pattern p = Pattern.compile("\\d{2}:\\d{2}:\\d{2}");
			Matcher m = p.matcher(str);
			if(m.find()){
				String time = m.group(0); //Format like 23:52:13
				int interval = Integer.valueOf(time.split(":")[1])/5;
				return String.valueOf(interval);
			}else{
				return NOT_FOUND;
			}
		}catch(Exception e){
			throw new IOException("caught exception",e);
		}
	}

}
