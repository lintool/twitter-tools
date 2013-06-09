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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
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

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.index.IndexStatuses.StatusField;

/**
 * Reference implementation for searching statuses.
 */
public class SearchStatuses {
  private SearchStatuses() {}

  private static final String INDEX_OPTION = "index";
  private static final String QUERY_OPTION = "query";
  private static final String NUM_HITS_OPTION = "num_hits";
  private static final String MAX_ID_OPTION = "max_id";
  private static final String SIMILARITY_OPTION = "similarity";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("index location").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of hits to return").create(NUM_HITS_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("max id").create(MAX_ID_OPTION));
    options.addOption(OptionBuilder.withArgName("query").hasArg()
        .withDescription("query").create(QUERY_OPTION));
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

    if (!cmdline.hasOption(QUERY_OPTION) || !cmdline.hasOption(INDEX_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(SearchStatuses.class.getName(), options);
      System.exit(-1);
    }

    File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));
    if (!indexLocation.exists()) {
      System.err.println("Error: " + indexLocation + " does not exist!");
      System.exit(-1);
    }

    String queryText = cmdline.getOptionValue(QUERY_OPTION);
    
    int numHits = 10;
    try {
      if (cmdline.hasOption(NUM_HITS_OPTION)) {
        numHits = Integer.parseInt(cmdline.getOptionValue(NUM_HITS_OPTION));
      }
    } catch (NumberFormatException e) {
      System.err.println("Invalid " + NUM_HITS_OPTION + ": " + cmdline.getOptionValue(NUM_HITS_OPTION));
      System.exit(-1);
    }

    long maxId = Long.MAX_VALUE;
    try {
      if (cmdline.hasOption(MAX_ID_OPTION)) {
        maxId = Long.parseLong(cmdline.getOptionValue(MAX_ID_OPTION));
      }
    } catch (NumberFormatException e) {
      System.err.println("Invalid " + MAX_ID_OPTION + ": " + cmdline.getOptionValue(MAX_ID_OPTION));
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

    out.println("Using similarity: " + searcher.getSimilarity().toString());

    QueryParser p = new QueryParser(Version.LUCENE_41, IndexStatuses.StatusField.TEXT.name, IndexStatuses.ANALYZER);
    Query query = p.parse(queryText);
    Filter filter = NumericRangeFilter.newLongRange(StatusField.ID.name, 0L, maxId, true, true);

    out.println("Query: " + query);

    TopDocs rs = searcher.search(query, filter, numHits);

    for (ScoreDoc scoreDoc : rs.scoreDocs) {
      Document hit = searcher.doc(scoreDoc.doc);
      System.out.println(hit);
      Field epoch = (Field) hit.getField(IndexStatuses.StatusField.EPOCH.name);

      out.println(String.format("%s\t%s\t%s\t%s\t%s",
          scoreDoc.score,
          hit.getField(IndexStatuses.StatusField.ID.name).stringValue(),
          hit.getField(IndexStatuses.StatusField.SCREEN_NAME.name).stringValue(),
          (epoch == null ? "" : epoch.stringValue()),
          hit.getField(IndexStatuses.StatusField.TEXT.name).stringValue()));
    }

    reader.close();
  }
}
