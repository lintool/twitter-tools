package cc.twittertools.piggybank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

// Sample usage: cc.twittertools.piggybank.GetLongitude($0#'geo'#'coordinates');
public class GetLongitude extends EvalFunc<String> {
  public String exec(Tuple input) throws IOException {
    DataBag bag = (DataBag) input.get(0);
    Iterator<Tuple> it = bag.iterator();
    if (!it.hasNext()) {
      return null;
    }
    it.next();
    if (!it.hasNext()) {
      return null;
    }

    Tuple tup = it.next();

    return (String) tup.get(0);
  }
}