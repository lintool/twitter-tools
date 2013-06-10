package cc.twittertools.search.local;

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

  // Defaults: if user doesn't specify an actual query, run MB01 as a demo.
  private static final String DEFAULT_QID = "MB01";
  private static final String DEFAULT_Q = "BBC World Service staff cuts";
  private static final long DEFAULT_MAX_ID = 34952194402811905L;
  private static final int DEFAULT_NUM_RESULTS = 10;
  private static final String DEFAULT_RUNTAG = "lucene4lm";

  private static final String INDEX_OPTION = "index";
  private static final String QID_OPTION = "qid";
  private static final String QUERY_OPTION = "q";
  private static final String RUNTAG_OPTION = "runtag";
  private static final String MAX_ID_OPTION = "max_id";
  private static final String NUM_HITS_OPTION = "num_hits";
  private static final String SIMILARITY_OPTION = "similarity";
  private static final String VERBOSE_OPTION = "verbose";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("index location").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("query id").create(QID_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("query text").create(QUERY_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("runtag").create(RUNTAG_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("maxid").create(MAX_ID_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of hits").create(NUM_HITS_OPTION));
    options.addOption(OptionBuilder.withArgName("similarity").hasArg()
        .withDescription("similarity to use (BM25, LM)").create(SIMILARITY_OPTION));
    options.addOption(new Option(VERBOSE_OPTION, "print out complete document"));

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

    String qid = cmdline.hasOption(QID_OPTION) ?
        cmdline.getOptionValue(QID_OPTION) : DEFAULT_QID;
    String queryText = cmdline.hasOption(QUERY_OPTION) ?
        cmdline.getOptionValue(QUERY_OPTION) : DEFAULT_Q;
    String runtag = cmdline.hasOption(RUNTAG_OPTION) ?
        cmdline.getOptionValue(RUNTAG_OPTION) : DEFAULT_RUNTAG;
    long maxId = cmdline.hasOption(MAX_ID_OPTION) ?
        Long.parseLong(cmdline.getOptionValue(MAX_ID_OPTION)) : DEFAULT_MAX_ID;
    int numHits = cmdline.hasOption(NUM_HITS_OPTION) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_HITS_OPTION)) : DEFAULT_NUM_RESULTS;
    boolean verbose = cmdline.hasOption(VERBOSE_OPTION);

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

    QueryParser p = new QueryParser(Version.LUCENE_41, IndexStatuses.StatusField.TEXT.name, IndexStatuses.ANALYZER);
    Query query = p.parse(queryText);
    Filter filter = NumericRangeFilter.newLongRange(StatusField.ID.name, 0L, maxId, true, true);

    TopDocs rs = searcher.search(query, filter, numHits);

    int i = 1;
    for (ScoreDoc scoreDoc : rs.scoreDocs) {
      Document hit = searcher.doc(scoreDoc.doc);

      out.println(String.format("%s Q0 %s %d %f %s", qid,
          hit.getField(StatusField.ID.name).stringValue(), i, scoreDoc.score, runtag));
      if (verbose) {
        out.println("# " + hit.toString().replaceAll("[\\n\\r]+", " "));
      }
      i++;
    }

    reader.close();
    out.close();
  }
}
