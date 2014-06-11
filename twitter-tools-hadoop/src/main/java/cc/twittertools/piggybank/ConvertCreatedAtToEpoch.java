package cc.twittertools.piggybank;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ConvertCreatedAtToEpoch extends EvalFunc<Long> {
  private static final String DATE_FORMAT = "EEE MMM d k:m:s ZZZZZ yyyy"; // "Fri Mar 29 11:03:41 +0000 2013";
  private static final SimpleDateFormat DATE_PARSER = new SimpleDateFormat(DATE_FORMAT);

  public Long exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0) {
      return -1L;
    }

    String createdAt = (String) input.get(0);
    long epoch;
    try {
      epoch = DATE_PARSER.parse(createdAt).getTime() / 1000;
    } catch (ParseException e) {
      epoch = -1L;
    }

    return epoch;
  }
}