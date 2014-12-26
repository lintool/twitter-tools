package cc.twittertools.udf;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class GetDayDiff extends EvalFunc<Long>{

	public Long exec(Tuple input) throws IOException {
		if(input == null || input.size() == 0){
			return null;
		}
		//Standard Time Format: Tue Feb 08 23:59:59 +0000 2011
		try{
			String str = (String) input.get(0);
			//Date base = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").parse("2013-04-01 00:00:00 +0000");
			Date base = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").parse("2011-01-23 00:00:00 +0000");
			Date current = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy").parse(str);
			long diff = current.getTime() - base.getTime();
			return TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
		}catch(Exception e){
			throw new IOException("caught exception",e);
		}
	}

}
