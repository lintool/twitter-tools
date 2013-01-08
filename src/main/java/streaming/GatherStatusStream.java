package streaming;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.varia.LevelRangeFilter;

import twitter4j.RawStreamListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public final class GatherStatusStream {
  private static int cnt = 0;

  private static final String MINUTE_ROLL = "'.'yyyy-MM-dd-HH-mm";
  private static final String HOUR_ROLL = "'.'yyyy-MM-dd-HH";

  public static void main(String[] args) throws TwitterException {
    PatternLayout layoutStandard = new PatternLayout();
    layoutStandard.setConversionPattern("[%p] %d %c %M - %m%n");

    PatternLayout layoutSimple = new PatternLayout();
    layoutSimple.setConversionPattern("%m%n");

    // Filter for the statuses: we only want INFO messages
    LevelRangeFilter filter = new LevelRangeFilter();
    filter.setLevelMax(Level.INFO);
    filter.setLevelMin(Level.INFO);
    filter.setAcceptOnMatch(true);
    filter.activateOptions();

    DailyRollingFileAppender statusesAppender = new DailyRollingFileAppender();
    statusesAppender.setFile("statuses.log");
    statusesAppender.setDatePattern(HOUR_ROLL);
    statusesAppender.setLayout(layoutSimple);
    statusesAppender.addFilter(filter);
    statusesAppender.activateOptions();

    DailyRollingFileAppender warningsAppender = new DailyRollingFileAppender();
    warningsAppender.setFile("warnings.log");
    warningsAppender.setDatePattern(HOUR_ROLL);
    warningsAppender.setLayout(layoutStandard);
    warningsAppender.setThreshold(Level.WARN);
    warningsAppender.activateOptions();

    ConsoleAppender consoleAppender = new ConsoleAppender();
    consoleAppender.setThreshold(Level.WARN);
    consoleAppender.setLayout(layoutStandard);
    consoleAppender.activateOptions();

    // configures the root logger
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.INFO);
    rootLogger.removeAllAppenders();
    rootLogger.addAppender(consoleAppender);
    rootLogger.addAppender(statusesAppender);
    rootLogger.addAppender(warningsAppender);

    // creates a custom logger and log messages
    final Logger logger = Logger.getLogger(GatherStatusStream.class);


    TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
    RawStreamListener rawListener = new RawStreamListener() {

      @Override
      public void onMessage(String rawString) {
        cnt++;
        logger.info(rawString);
        if (cnt % 1000 == 0) {
          System.out.println(cnt + " messages received.");
        }
      }

      @Override
      public void onException(Exception ex) {
        logger.warn(ex);
      }

    };

    twitterStream.addListener(rawListener);
    twitterStream.sample();
  }
}
