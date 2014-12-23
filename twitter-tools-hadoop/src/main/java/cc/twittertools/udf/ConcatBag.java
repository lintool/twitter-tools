package cc.twittertools.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class ConcatBag extends EvalFunc<String>{
  
  public String exec(DataBag input) throws IOException {
    if(input == null || input.size() == 0){
      return null;
    }
    String output = "";
    for(Tuple tuple: input) {
      String word = (String) tuple.get(0);
      output += word + " ";
    }
    return output;
  }

  @Override
  public String exec(Tuple arg0) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
}
