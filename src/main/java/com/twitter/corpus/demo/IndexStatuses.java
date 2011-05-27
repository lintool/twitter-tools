package com.twitter.corpus.demo;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.twitter.corpus.data.HtmlStatusBlockReader;
import com.twitter.corpus.data.HtmlStatusCorpusReader;
import com.twitter.corpus.data.JsonStatusBlockReader;
import com.twitter.corpus.data.JsonStatusCorpusReader;
import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusStream;

/**
 * Reference implementation for indexing statuses.
 */
public class IndexStatuses {
  private static final Logger LOG = Logger.getLogger(IndexStatuses.class);

  public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_31);

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

  private static final String HTML_MODE = "html";
  private static final String JSON_MODE = "json";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input directory or file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("index location").create(INDEX_OPTION));
    options.addOption(HTML_MODE, false, "input is HTML SequenceFile; mutually exclusive with -" + JSON_MODE);
    options.addOption(JSON_MODE, false, "input is JSON; mutually exclusive with -" + HTML_MODE);

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!(cmdline.hasOption(INPUT_OPTION) && cmdline.hasOption(INDEX_OPTION) && (cmdline
        .hasOption(HTML_MODE) ^ cmdline.hasOption(JSON_MODE)))) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(IndexStatuses.class.getName(), options);
      System.exit(-1);
    }

    File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));

    LOG.info("Indexing " + cmdline.getOptionValue(INPUT_OPTION));
    StatusStream stream;
    // Figure out if we're reading from HTML SequenceFiles or JSON.
    if (cmdline.hasOption(HTML_MODE)) {
      FileSystem fs = FileSystem.get(new Configuration());
      Path file = new Path(cmdline.getOptionValue(INPUT_OPTION));
      if (!fs.exists(file)) {
        System.err.println("Error: " + file + " does not exist!");
        System.exit(-1);
      }

      if (fs.getFileStatus(file).isDir()) {
        stream = new HtmlStatusCorpusReader(file, fs);
      } else {
        stream = new HtmlStatusBlockReader(file, fs);
      }
    } else {
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
    }

    Analyzer analyzer = ANALYZER;
    Similarity similarity = new ConstantNormSimilarity();
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_31, analyzer);
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
      LOG.info("Optimizing index...");
      writer.optimize();
      writer.close();
    } finally {
      stream.close();
    }

    LOG.info(String.format("Total of %s statuses indexed", cnt));
  }

  public static class ConstantNormSimilarity extends DefaultSimilarity {
    private static final long serialVersionUID = 2737920231537795826L;

    @Override
    public float computeNorm(String field, FieldInvertState state) {
      return 1.0f;
    }
  }
}
