package cc.twittertools.search.retrieval;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
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

import cc.twittertools.search.configuration.IndriQueryParams;
import cc.twittertools.search.configuration.IndriRunParams;

public class QueryEnvironment {
  private String pathToRunParamFile;
  private String pathToQueryFile;
  private String similarity = "lm";

  private String pathToIndex; // i.e. the actual index location. specified in pathToRunParamFile
  private Queries queries;
  private int count = 0;

  @SuppressWarnings("unused")
  private int fbDocs = 0;
  @SuppressWarnings("unused")
  private int fbTerms = 0;
  @SuppressWarnings("unused")
  private double fbOrigWeight = 0.0;

  private QueryParser queryParser;
  private IndexSearcher searcher;

  public static enum DocField {
    TEXT("text"),
    TIME("id"),
    DATE("createdAt");

    public final String name;

    DocField(String s) {
      name = s;
    }
  };

  public void runQueries() throws IOException {
    IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(pathToIndex)));
    searcher = new IndexSearcher(reader);

    if (similarity.equalsIgnoreCase("bm25")) {
      Similarity simBM25 = new BM25Similarity();
      searcher.setSimilarity(simBM25);
    } else if (similarity.equalsIgnoreCase("lm")) {
      NamedList<Double> paramNamedList = new NamedList<Double>();
      paramNamedList.add("mu", 2500.0);
      SolrParams params = SolrParams.toSolrParams(paramNamedList);
      LMDirichletSimilarityFactory factory = new LMDirichletSimilarityFactory();
      factory.init(params);
      Similarity simLMDir = factory.getSimilarity();
      searcher.setSimilarity(simLMDir);
    }

    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_41);

    queryParser = new QueryParser(Version.LUCENE_41, DocField.TEXT.name, analyzer);

    cc.twittertools.search.retrieval.Query query = null;
    while ((query = queries.getNextQuery()) != null) {
      System.err.println(query.getQueryName());

      try {
        Query luceneQuery = queryParser.parse(query.getQueryString());
        Filter filter = NumericRangeFilter.newLongRange(DocField.TIME.name, 0L,
            Long.parseLong(query.getMetadataField("lastrel")), true, true);

        this.runQuery(query.getQueryName(), luceneQuery, filter, count);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    reader.close();
  }

  public void runQuery(String queryName, Query query, Filter filter, int count) {
    try {

      System.err.println(query);

      TopDocs rs = searcher.search(query, filter, count);

      int j = 1;
      for (ScoreDoc scoreDoc : rs.scoreDocs) {
        Document hit = searcher.doc(scoreDoc.doc);

        System.out.println(queryName + " Q0 " + hit.getField("id").numericValue() + " " + (j++)
            + " " + scoreDoc.score + " lucy");

      }

    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public void parseParams() throws Exception {

    // make sure we've got a file specifying the index to search and a file with
    // queries to run

    // first the index
    if (pathToRunParamFile == null) {
      System.err.println("QueryEnvironment: pathToRunParamFile not set!");
      throw new IOException();
    }
    File paramFile = new File(pathToRunParamFile);
    if (!paramFile.exists()) {
      System.err.println("QueryEnvironment: pathToRunParamFile does not exist!");
      throw new IOException();
    }

    IndriRunParams runParams = new IndriRunParams();
    runParams.ParseXMLQueryFile(pathToRunParamFile);

    similarity = runParams.getSimilarity();
    if (similarity == null) {
      similarity = "lm";
    }

    pathToIndex = runParams.getIndexName();
    if (pathToIndex == null) {
      System.err.println("IndexEnvironment: null value for index name!");
      throw new IllegalArgumentException();
    }
    count = runParams.getCount();
    if (count < 1) {
      System.err.println("cannot retrieve " + count + " docs!");
      System.exit(-1);
    }
    fbDocs = runParams.getFBDocs();
    fbTerms = runParams.getFBTerms();

    System.err.println("index:  " + pathToIndex);

    // now the queries
    if (pathToQueryFile == null) {
      System.err.println("QueryEnvironment: pathToQueryFile not set!");
      throw new IOException();
    }
    File queryFile = new File(pathToQueryFile);
    if (!queryFile.exists()) {
      System.err.println("QueryEnvironment: pathToQueryFile does not exist!");
      throw new IOException();
    }

    IndriQueryParams queryParams = new IndriQueryParams();
    queryParams.ParseXMLQueryFile(pathToQueryFile);

    queries = queryParams.getQueries();
    System.err.println("read " + queries.getNumQueries() + " queries.");

  }

  public void setPathToIndexFile(String pathToRunParamFile) {
    this.pathToRunParamFile = pathToRunParamFile;
  }

  public void setPathToQueryFile(String setPathToQueryFile) {
    this.pathToQueryFile = setPathToQueryFile;
  }

}
