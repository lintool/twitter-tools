package cc.twittertools.piggybank;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class IsMap extends FilterFunc {

  @Override
  public Boolean exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0) {
      return false;
    }

    return (input.get(0) instanceof Map);
  }
}