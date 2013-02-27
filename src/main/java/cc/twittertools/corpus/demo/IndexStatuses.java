package cc.twittertools.corpus.demo;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.similarities.LMDirichletSimilarityFactory;

import cc.twittertools.corpus.data.JsonStatusBlockReader;
import cc.twittertools.corpus.data.JsonStatusCorpusReader;
import cc.twittertools.corpus.data.Status;
import cc.twittertools.corpus.data.StatusStream;

/**
 * Reference implementation for indexing statuses.
 */
public class IndexStatuses {
  private static final Logger LOG = Logger.getLogger(IndexStatuses.class);

  public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_41);

  private IndexStatuses() {}

  public static enum StatusField {
    ID("id"),
    SCREEN_NAME("screen_name"),
    CREATED_AT("create_at"),
    TEXT("text"),
    DAY("day");

    public final String name;

    StatusField(String s) {
      name = s;
    }
  };

  private static final String INPUT_OPTION = "input";
  private static final String INDEX_OPTION = "index";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input directory or file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("index location").create(INDEX_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!(cmdline.hasOption(INPUT_OPTION) && cmdline.hasOption(INDEX_OPTION))) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(IndexStatuses.class.getName(), options);
      System.exit(-1);
    }

    File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));

    LOG.info("Indexing " + cmdline.getOptionValue(INPUT_OPTION));
    StatusStream stream;

    File file = new File(cmdline.getOptionValue(INPUT_OPTION));
    if (!file.exists()) {
      System.err.println("Error: " + file + " does not exist!");
      System.exit(-1);
    }

    if (file.isDirectory()) {
      stream = new JsonStatusCorpusReader(file);
    } else {
      stream = new JsonStatusBlockReader(file);
    }

    NamedList paramNamedList = new NamedList();
    paramNamedList.add("mu", 2500.0);
    
    SolrParams params = SolrParams.toSolrParams(paramNamedList);
    LMDirichletSimilarityFactory factory = new LMDirichletSimilarityFactory();
    factory.init(params);
    Similarity similarity = factory.getSimilarity();
   

    Analyzer analyzer = ANALYZER;
    

    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_41, analyzer);
    config.setSimilarity(similarity);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE); // Overwrite existing.

    IndexWriter writer = new IndexWriter(FSDirectory.open(indexLocation), config);

    int cnt = 0;
    Status status;
    try {
      while ((status = stream.next()) != null) {
        if (status.getText() == null) {
          continue;
        }

        cnt++;
        String createdAt = status.getCreatedAt();
        Document doc = new Document();
        doc.add(new Field(StatusField.ID.name, status.getId() + "",
            Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(StatusField.SCREEN_NAME.name, status.getScreenname(),
            Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(StatusField.CREATED_AT.name, createdAt, Store.YES, Index.NO));
        doc.add(new Field(StatusField.TEXT.name, status.getText(), Store.YES, Index.ANALYZED));

        String[] arr = createdAt.split(" ");
        String createDay = new StringBuffer().append(arr[1]).append("_").append(arr[2]).toString();
        doc.add(new Field(StatusField.DAY.name, createDay, Store.YES, Index.NOT_ANALYZED_NO_NORMS));

        writer.addDocument(doc);
        if (cnt % 10000 == 0) {
          LOG.info(cnt + " statuses indexed");
        }
      }
      
      
      LOG.info("(as of Lucene 4.1, Not Optimizing index...");
      
      // deprecated in lucene 4?
      //writer.optimize();
      
      writer.close();
    } finally {
      stream.close();
    }

    LOG.info(String.format("Total of %s statuses indexed", cnt));
  }

  public static class ConstantNormSimilarity extends LMDirichletSimilarity {
    private static final long serialVersionUID = 2737920231537795826L;

    public float computeNorm(String field, FieldInvertState state) {
      return 1.0f;
    }
  }
}
