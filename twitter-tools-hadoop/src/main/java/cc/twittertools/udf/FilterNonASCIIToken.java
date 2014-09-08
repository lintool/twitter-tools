package cc.twittertools.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.google.common.base.CharMatcher;

public class FilterNonASCIIToken extends EvalFunc<Boolean>{
	
	public Boolean exec(Tuple input) throws IOException {
		if(input == null || input.size() == 0){
			return false;
		}
		String token = (String) input.get(0);
		boolean isAscii = CharMatcher.ASCII.matchesAllOf(token);
		boolean isEmpty = (token.length() != 0);
		return isAscii && isEmpty;
	}
	
}
