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
		String[] months ={"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
		try{
			String str = (String) input.get(0);
			String[] groups = str.split("\\s+");
			String year = groups[5];
			String month = groups[1];
			String monthpart=null;
			int index;
			for(index=0; index<months.length; index++){
				if(months[index].equals(month)){
					if(index+1>=10)
						monthpart = String.valueOf(index+1);
					else
						monthpart = "0" + String.valueOf(index+1);
					break;
				}
			}
			if(index == months.length){
				throw new Exception("month not found");
			}
			return year+monthpart;
		}catch(Exception e){
			throw new IOException("caught exception",e);
		}
	}

}
