package com.twitter.corpus.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;

/**
 * Reference implementation for searching statuses.
 */
public class BatchSearchStatuses {
  private BatchSearchStatuses() {}

  private static final String INDEX_OPTION = "index";
  private static final String QUERY_OPTION = "query";
  private static final String NUM_HITS_OPTION = "num_hits";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("index location").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of hits to return").create(NUM_HITS_OPTION));
    options.addOption(OptionBuilder.withArgName("queries").hasArg()
        .withDescription("file containing queries").create(QUERY_OPTION));

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
      formatter.printHelp(BatchSearchStatuses.class.getName(), options);
      System.exit(-1);
    }

    File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));
    if (!indexLocation.exists()) {
      System.err.println("Error: " + indexLocation + " does not exist!");
      System.exit(-1);
    }

    int numHits = 1000;
    try {
      if (cmdline.hasOption(NUM_HITS_OPTION)) {
        numHits = Integer.parseInt(cmdline.getOptionValue(NUM_HITS_OPTION));
      }
    } catch (NumberFormatException e) {
      System.err.println("Invalid " + NUM_HITS_OPTION + ": " + cmdline.getOptionValue(NUM_HITS_OPTION));
      System.exit(-1);
    }

    PrintStream out = new PrintStream(System.out, true, "UTF-8");

    Directory dir = new MMapDirectory(indexLocation);
    IndexReader reader = IndexReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
	QueryParser qparser = new QueryParser(Version.LUCENE_31, IndexStatuses.StatusField.TEXT.name,
			IndexStatuses.ANALYZER);

    String filename = cmdline.getOptionValue(QUERY_OPTION);
    BufferedReader qin = new BufferedReader(new FileReader(filename));

    String line = null;
    while ((line = qin.readLine()) != null) {
    	String[] parts = line.split(" ", 3);
    	
    	Query query = qparser.parse(parts[2]);
    	System.err.println("Query: " + query);

    	TopDocs rs = searcher.search(query, 
    			new TermRangeFilter(IndexStatuses.StatusField.ID.name,
    								null, parts[1], false, true),
    			numHits);

    	int i = 0;
    	for (ScoreDoc scoreDoc : rs.scoreDocs) {
    		Document hit = searcher.doc(scoreDoc.doc);
    		
    		out.println(String.format("%s Q0 %s %d %6.4f run\n",
    				parts[0],
    				hit.getField(IndexStatuses.StatusField.ID.name).stringValue(),
    				i++,
    				scoreDoc.score));
    	}
    }
    
    searcher.close();
    reader.clone();
  }
}
