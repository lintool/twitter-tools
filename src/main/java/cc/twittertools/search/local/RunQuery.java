package cc.twittertools.search.local;

import java.io.File;
import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
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
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.similarities.LMDirichletSimilarityFactory;

import cc.twittertools.index.IndexStatuses.StatusField;
import cc.twittertools.search.TrecTopic;
import cc.twittertools.search.TrecTopicSet;

public class RunQuery {

  private RunQuery() {}

  private static final String INDEX_OPTION = "index";
  private static final String TOPICS_OPTION = "queries";
  private static final String NUM_HITS_OPTION = "num_hits";
  private static final String SIMILARITY_OPTION = "similarity";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("index location").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of hits to return").create(NUM_HITS_OPTION));
    options.addOption(OptionBuilder.withArgName("query").hasArg()
        .withDescription("query").create(TOPICS_OPTION));
    options.addOption(OptionBuilder.withArgName("similarity").hasArg()
        .withDescription("similarity to use (BM25, LM)").create(SIMILARITY_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(TOPICS_OPTION) || !cmdline.hasOption(INDEX_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(SearchStatuses.class.getName(), options);
      System.exit(-1);
    }

    File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));
    if (!indexLocation.exists()) {
      System.err.println("Error: " + indexLocation + " does not exist!");
      System.exit(-1);
    }

    String topicsFile = cmdline.getOptionValue(TOPICS_OPTION);
    
    int numHits = 1000;
    try {
      if (cmdline.hasOption(NUM_HITS_OPTION)) {
        numHits = Integer.parseInt(cmdline.getOptionValue(NUM_HITS_OPTION));
      }
    } catch (NumberFormatException e) {
      System.err.println("Invalid " + NUM_HITS_OPTION + ": " + cmdline.getOptionValue(NUM_HITS_OPTION));
      System.exit(-1);
    }

    String similarity = "LM";
    if (cmdline.hasOption(SIMILARITY_OPTION)) {
      similarity = cmdline.getOptionValue(SIMILARITY_OPTION);
    }

    PrintStream out = new PrintStream(System.out, true, "UTF-8");

    IndexReader reader = DirectoryReader.open(FSDirectory.open(indexLocation));
    IndexSearcher searcher = new IndexSearcher(reader);

    if (similarity.equalsIgnoreCase("BM25")) {
      Similarity simBM25 = new BM25Similarity();
      searcher.setSimilarity(simBM25);
    } else if (similarity.equalsIgnoreCase("LM")) {
      NamedList<Double> paramNamedList = new NamedList<Double>();
      paramNamedList.add("mu", 2500.0);
      SolrParams params = SolrParams.toSolrParams(paramNamedList);
      LMDirichletSimilarityFactory factory = new LMDirichletSimilarityFactory();
      factory.init(params);
      Similarity simLMDir = factory.getSimilarity();
      searcher.setSimilarity(simLMDir);
    }

    //out.println("Using similarity: " + searcher.getSimilarity().toString());
    QueryParser p = new QueryParser(Version.LUCENE_41, StatusField.TEXT.name,
        new StandardAnalyzer(Version.LUCENE_41));
        //IndexStatuses.ANALYZER);

    TrecTopicSet topics = TrecTopicSet.fromFile(topicsFile);
    for ( TrecTopic topic : topics ) {
      Query query = p.parse(topic.getQuery());
      Filter filter = NumericRangeFilter.newLongRange(StatusField.ID.name, 0L,
          topic.getQueryTweetTime(), true, true);

      //out.println("Query: " + query);

      TopDocs rs = searcher.search(query, filter, numHits);

      int i = 1;
      for (ScoreDoc scoreDoc : rs.scoreDocs) {
        Document hit = searcher.doc(scoreDoc.doc);
        out.println(topic.getId() + " Q0 " + 
        hit.getField(StatusField.ID.name).stringValue() + " " + i + " " + scoreDoc.score + " lucy");
        i++;
      }
    }
    reader.close();
  }

}
