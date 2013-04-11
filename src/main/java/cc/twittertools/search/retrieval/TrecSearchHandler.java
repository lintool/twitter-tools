package cc.twittertools.search.retrieval;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.similarities.LMDirichletSimilarityFactory;

import cc.twittertools.search.indexing.IndexStatuses.StatusField;
import cc.twittertools.search.retrieval.QueryEnvironment.DocField;
import cc.twittertools.thrift.gen.TrecSearch;
import cc.twittertools.thrift.gen.TrecSearchException;
import cc.twittertools.thrift.gen.TrecSearchQuery;
import cc.twittertools.thrift.gen.TrecSearchResult;

public class TrecSearchHandler implements TrecSearch.Iface {
  private IndexSearcher searcher;
  private static Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_41);
  private static QueryParser queryParser = new QueryParser(Version.LUCENE_41, DocField.TEXT.name, analyzer);

  private static final int MAX_RESULTS = 1000;

  public TrecSearchHandler(String indexPath) {
    try {
      IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
      searcher = new IndexSearcher(reader);

      NamedList paramNamedList = new NamedList();
      paramNamedList.add("mu", 2500.0);
      SolrParams params = SolrParams.toSolrParams(paramNamedList);
      LMDirichletSimilarityFactory factory = new LMDirichletSimilarityFactory();
      factory.init(params);
      Similarity simLMDir = factory.getSimilarity();
      searcher.setSimilarity(simLMDir);
    } catch (IOException e) {

    }
  }

  public List<TrecSearchResult> search(TrecSearchQuery query) throws TrecSearchException {
    List<TrecSearchResult> results = new ArrayList<TrecSearchResult>();
    long startTime = System.currentTimeMillis();
    try {
      Filter filter = NumericRangeFilter.newLongRange(DocField.TIME.name, 0L, 
          query.max_uid, true, true);
      
      Query q = queryParser.parse(query.text);
      TopDocs rs = searcher.search(q, filter, query.num_results);
      for (ScoreDoc scoreDoc : rs.scoreDocs) {
        Document hit = searcher.doc(scoreDoc.doc);
        
        TrecSearchResult p = new TrecSearchResult();
        p.id = Long.parseLong(hit.get(StatusField.ID.name));
        p.screen_name = hit.get(StatusField.SCREEN_NAME.name);
        p.created_at = hit.get(StatusField.CREATED_AT.name);
        p.text = hit.get(StatusField.TEXT.name);
        p.rsv = scoreDoc.score;

        results.add(p);
      }
    } catch (Exception e) {
      throw new TrecSearchException(e.getMessage());
    }
    long endTime = System.currentTimeMillis();
    System.out.println("Query took " + (endTime-startTime) + " ms");
    return results;
  }
}