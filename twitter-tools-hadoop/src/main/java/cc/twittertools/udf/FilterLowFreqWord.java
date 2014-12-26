package cc.twittertools.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.google.common.base.CharMatcher;

public class FilterLowFreqWord extends EvalFunc<Boolean>{
			
		public Boolean exec(Tuple input) throws IOException {
			if(input == null || input.size() == 0){
				return false;
			}
			int freq = (Integer) input.get(0);
			return freq > 1;
		}
}
