package umd.twittertools.run;

import java.io.File;
import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import umd.twittertools.data.TrecTopic;
import umd.twittertools.data.TrecTopicSet;
import umd.twittertools.data.Tweet;
import umd.twittertools.data.TweetSet;
import umd.twittertools.model.QueryLikelihoodModelIndex;
import cc.twittertools.index.IndexStatuses;
import cc.twittertools.index.IndexStatuses.StatusField;

public class RunQueriesIndex {
  private static final String DEFAULT_RUNTAG = "lucene4lm";

  private static final String INDEX_OPTION = "index";
  private static final String QUERIES_OPTION = "queries";
  private static final String NUM_RESULTS_OPTION = "num_results";
  private static final String SIMILARITY_OPTION = "similarity";
  private static final String RUNTAG_OPTION = "runtag";
  private static final String VERBOSE_OPTION = "verbose";

  private RunQueriesIndex() {}

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("index location").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of results to return").create(NUM_RESULTS_OPTION));
    options.addOption(OptionBuilder.withArgName("file").hasArg()
        .withDescription("file containing topics in TREC format").create(QUERIES_OPTION));
    options.addOption(OptionBuilder.withArgName("similarity").hasArg()
        .withDescription("similarity to use (BM25, LM)").create(SIMILARITY_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("runtag").create(RUNTAG_OPTION));
    options.addOption(new Option(VERBOSE_OPTION, "print out complete document"));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(QUERIES_OPTION) || !cmdline.hasOption(INDEX_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(RunQueriesIndex.class.getName(), options);
      System.exit(-1);
    }

    File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));
    if (!indexLocation.exists()) {
      System.err.println("Error: " + indexLocation + " does not exist!");
      System.exit(-1);
    }

    String runtag = cmdline.hasOption(RUNTAG_OPTION) ?
        cmdline.getOptionValue(RUNTAG_OPTION) : DEFAULT_RUNTAG;

    String topicsFile = cmdline.getOptionValue(QUERIES_OPTION);
    
    int numResults = 1000;
    try {
      if (cmdline.hasOption(NUM_RESULTS_OPTION)) {
        numResults = Integer.parseInt(cmdline.getOptionValue(NUM_RESULTS_OPTION));
      }
    } catch (NumberFormatException e) {
      System.err.println("Invalid " + NUM_RESULTS_OPTION + ": " + cmdline.getOptionValue(NUM_RESULTS_OPTION));
      System.exit(-1);
    }

    String similarity = "LM";
    if (cmdline.hasOption(SIMILARITY_OPTION)) {
      similarity = cmdline.getOptionValue(SIMILARITY_OPTION);
    }

    boolean verbose = cmdline.hasOption(VERBOSE_OPTION);

    PrintStream out = new PrintStream(System.out, true, "UTF-8");

    IndexReader reader = DirectoryReader.open(FSDirectory.open(indexLocation));
    IndexSearcher searcher = new IndexSearcher(reader);

    if (similarity.equalsIgnoreCase("BM25")) {
      searcher.setSimilarity(new BM25Similarity());
    } else if (similarity.equalsIgnoreCase("LM")) {
      searcher.setSimilarity(new LMDirichletSimilarity(2500.0f));
    }
    
    QueryLikelihoodModelIndex model = new QueryLikelihoodModelIndex(reader);
    QueryParser p = new QueryParser(Version.LUCENE_43, StatusField.TEXT.name,
        IndexStatuses.ANALYZER);

    TrecTopicSet topics = TrecTopicSet.fromFile(new File(topicsFile));
    for (TrecTopic topic : topics) {
      Query query = p.parse(topic.getQuery());
      Filter filter = NumericRangeFilter.newLongRange(StatusField.ID.name, 0L,
          topic.getQueryTweetTime(), true, true);
      
      Integer qid = topic.getId();
			long queryTime = topic.getQueryTime();
			TweetSet tweetSet = new TweetSet(topic.getQuery(), qid);
      
      TopDocs rs = searcher.search(query, filter, numResults);

      int i = 1;
      for (ScoreDoc scoreDoc : rs.scoreDocs) {
        Document hit = searcher.doc(scoreDoc.doc);
        String text = hit.get(StatusField.TEXT.name);
        Long id = (Long) hit.getField(StatusField.ID.name).numericValue();
        Long epoch = (Long) hit.getField(StatusField.EPOCH.name).numericValue();
        Long retweeted_status_id = 0L;
        if ( hit.get(StatusField.RETWEETED_STATUS_ID.name) != null) {
          retweeted_status_id = (Long) hit.getField(StatusField.RETWEETED_STATUS_ID.name).numericValue();
        }
        double qlScore = model.computeQLScore(topic.getQuery(), text);
        
        if (retweeted_status_id != 0) {
					continue; // filter all retweets
				}
        Tweet tweet = new Tweet(id, epoch, queryTime - epoch, qlScore);
        if (verbose) {
          tweet.setText(text);
        }
        tweetSet.add(tweet);       
        i++;
      }
      
      tweetSet.sortByQLscore();
    	
			int rank = 1, counter = 1;
			double prevScore = 0;
			for (Tweet tweet : tweetSet) {
				if (prevScore != tweet.getQlScore()) {
					rank = counter;
				} 
				tweet.setRank(rank);
				out.println(String.format("%s %d %d %d %d %f", qid, tweet.getId(), tweet.getRank(), 
						tweet.getEpoch(), tweet.getTimeDiff(), tweet.getQlScore()));
				if (verbose) {
					out.println("# " + tweet.getText());
				}
				counter++;
				prevScore = tweet.getQlScore();
			}
    }
    reader.close();
    out.close();
  }
}
