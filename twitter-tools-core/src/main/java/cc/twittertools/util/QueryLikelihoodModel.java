package cc.twittertools.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.Version;

import cc.twittertools.index.LowerCaseEntityPreservingFilter;
import cc.twittertools.index.LuceneTokenizer;

public class QueryLikelihoodModel {
  private static final String FIELD_TEXT = "text";
  private static final double mu = 2500.0;
  private static IndexReader index;
  private long totalTokens;
  
  public QueryLikelihoodModel(IndexReader index) throws IOException {
    this.index = index;
    this.totalTokens = index.getSumTotalTermFreq(FIELD_TEXT);
    System.out.println("total number of tokens:" + this.totalTokens);
  }
  
  public double computeQLScore(String query, String doc) throws IOException {
    double score = 0;
    List<String> queryTerms = LuceneTokenizer.tokenize(query);
    List<String> docTerms = LuceneTokenizer.tokenize(doc);
    
    int docLen = 0;
    Map<String, Integer> docTermCountMap = new HashMap<String, Integer>();
    for (String term: docTerms) {
      if (docTermCountMap.containsKey(term)) {
        docTermCountMap.put(term, docTermCountMap.get(term)+1);
      } else {
        docTermCountMap.put(term, 1);
      }
      docLen += 1;
    }
    
    for(String queryTerm: queryTerms) {
      Term term = new Term(FIELD_TEXT, queryTerm);
      long termFreqInCorpus = index.totalTermFreq(term);
      int termFreqInDoc = docTermCountMap.containsKey(queryTerm) ? docTermCountMap.get(queryTerm) : 0;
      score += Math.log((termFreqInDoc + mu*((double)termFreqInCorpus/totalTokens)) 
          / (docLen + mu));
      System.out.println("term: " + queryTerm + " freq in doc: " + termFreqInDoc
          + " freq in corpus: " + termFreqInCorpus + " total tokens: " + totalTokens);
    }
    return score;
  }
}
