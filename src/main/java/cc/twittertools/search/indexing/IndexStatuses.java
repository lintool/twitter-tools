package cc.twittertools.search.indexing;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import cc.twittertools.corpus.data.JsonStatusCorpusReader;
import cc.twittertools.corpus.data.Status;
import cc.twittertools.corpus.data.StatusStream;
import cc.twittertools.corpus.data.TSVStatusCorpusReader;

/**
 * Reference implementation for indexing statuses.
 */
public class IndexStatuses {
  private static final Logger LOG = Logger.getLogger(IndexStatuses.class);

  public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_41);
  public static String corpusFormat = null;

  private IndexStatuses() {
  }

  public static enum StatusField {
    ID("id"),
    SCREEN_NAME("screen_name"),
    CREATED_AT("created_at"),
    EPOCH("epoch"),
    TEXT("text"),
    LANG("lang"),
    IN_REPLY_TO_STATUS_ID("in_reply_to_status_id"),
    IN_REPLY_TO_USER_ID("in_reply_to_user_id"),
    FOLLOWERS_COUNT("followers_count"),
    FRIENDS_COUNT("friends_count"),
    STATUSES_COUNT("statuses_count"),
    latitude("latitude"),
    LONGITUDE("longitude"),
    RETWEETED_STATUS_ID("retweeted_status_id"),
    RETWEETED_USER_ID("retweeted_user_id"),
    RETWEET_COUNT("retweet_count");

    public final String name;

    StatusField(String s) {
      name = s;
    }
  };

  private static final String HELP_OPTION = "h";
  private static final String COLLECTION_OPTION = "collection";
  private static final String INDEX_OPTION = "index";
  private static final String JSON_OPTION = "json";
  private static final String TSV_OPTION = "tsv";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(HELP_OPTION, "show help"));
    options.addOption(new Option(JSON_OPTION, "input in JSON"));
    options.addOption(new Option(TSV_OPTION, "input in TSV"));
    options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("source collection directory").create(COLLECTION_OPTION));
    options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("index location").create(INDEX_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (cmdline.hasOption(HELP_OPTION) || !cmdline.hasOption(COLLECTION_OPTION)
        || !cmdline.hasOption(INDEX_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(IndexStatuses.class.getName(), options);
      System.exit(-1);
    }

    String collectionPath = cmdline.getOptionValue(COLLECTION_OPTION);
    String indexPath = cmdline.getOptionValue(INDEX_OPTION);

    long startTime = System.currentTimeMillis();

    StatusStream stream;

    File file = new File(collectionPath);
    if (!file.exists()) {
      System.err.println("Error: " + file + " does not exist!");
      System.exit(-1);
    }

    if (cmdline.hasOption(TSV_OPTION)) {
      stream = new TSVStatusCorpusReader(file);
    } else {
      stream = new JsonStatusCorpusReader(file);
    }

    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_41);

    Directory dir = FSDirectory.open(new File(indexPath));
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_41, analyzer);
    config.setOpenMode(OpenMode.CREATE);

    LOG.info("collection: " + collectionPath);
    LOG.info("index: " + indexPath);

    IndexWriter writer = new IndexWriter(dir, config);
    int cnt = 0;
    Status status;
    try {
      while ((status = stream.next()) != null) {
        if (status.getText() == null) {
          continue;
        }

        cnt++;
        Document doc = new Document();
        doc.add(new LongField(StatusField.ID.name, status.getId(), Field.Store.YES));
        doc.add(new LongField(StatusField.EPOCH.name, status.getEpoch(), Field.Store.YES));
        doc.add(new TextField(StatusField.SCREEN_NAME.name, status.getScreenname(), Store.YES));
        //doc.add(new TextField(StatusField.CREATED_AT.name, status.getCreatedAt(), Store.YES));
        doc.add(new TextField(StatusField.TEXT.name, status.getText(), Store.YES));
        doc.add(new IntField(StatusField.RETWEET_COUNT.name, status.getRetweetCount(), Store.YES));

        // the following tests for presence or absence of fields are going to slow down indexing a lot.
        
        long inReplyToStatusId = status.getInReplyToStatusId();
        if(inReplyToStatusId > 0) {
          doc.add(new LongField(StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId, Field.Store.YES));
          doc.add(new LongField(StatusField.IN_REPLY_TO_USER_ID.name, status.getInReplyToUserId(), Field.Store.YES));
        }
        
        String lang = status.getLang();
        if(! lang.equals("unknown")) {
          doc.add(new TextField(StatusField.LANG.name, status.getLang(), Store.YES));
        }
        
        /*
        double latitude = status.getlatitude();
        if(! Double.isInfinite(latitude)) {
          doc.add(new DoubleField(StatusField.latitude.name, latitude, Field.Store.YES));
          doc.add(new DoubleField(StatusField.LONGITUDE.name, status.getLongitude(), Field.Store.YES));          
        }
        */
        
        
        long retweetStatusId = status.getRetweetedStatusId();
        if(retweetStatusId > 0) {
          doc.add(new LongField(StatusField.RETWEETED_STATUS_ID.name, retweetStatusId, Field.Store.YES));
          doc.add(new LongField(StatusField.RETWEETED_USER_ID.name, status.getRetweetedUserId(), Field.Store.YES));
        }
        
        
        doc.add(new IntField(StatusField.FRIENDS_COUNT.name, status.getFollowersCount(), Store.YES));
        doc.add(new IntField(StatusField.FOLLOWERS_COUNT.name, status.getFriendsCount(), Store.YES));
        doc.add(new IntField(StatusField.STATUSES_COUNT.name, status.getStatusesCount(), Store.YES));
        

        
        
        writer.addDocument(doc);
        if (cnt % 100000 == 0) {
          LOG.info(cnt + " statuses indexed");
        }
      }

      LOG.info(String.format("Total of %s statuses added", cnt));
      LOG.info("Merging segments...");
      writer.forceMerge(1);
      LOG.info("Done!");
      LOG.info("Total elapsed time: " + (System.currentTimeMillis() - startTime) + "ms");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      writer.close();
      dir.close();
      stream.close();
    }
  }
}
