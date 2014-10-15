package cc.twittertools.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

import cc.twittertools.index.LowerCaseEntityPreservingFilter;
import cc.twittertools.index.TweetAnalyzer;

public class QueryLikelihoodModel {
  private static final String FIELD_TEXT = "text";
  private static final double mu = 2500.0;
  private static IndexReader index;
  private static TweetAnalyzer tokenizer;
  private long totalTokens;
  
  public QueryLikelihoodModel(IndexReader index) throws IOException {
    this.index = index;
    this.tokenizer = new TweetAnalyzer(Version.LUCENE_43, true);
    this.totalTokens = index.getSumTotalTermFreq(FIELD_TEXT);
    System.out.println("total number of tokens:" + this.totalTokens);
  }
  
  public double computeQLScore(String query, String doc) throws IOException {
    double score = 0;
    TokenStream queryStream = tokenizer.tokenStream(FIELD_TEXT, new StringReader(query));
    List<String> queryTerms = tokenize(queryStream);
    TokenStream docStream = tokenizer.tokenStream(FIELD_TEXT, new StringReader(doc));
    List<String> docTerms = tokenize(docStream);
    
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
    
    System.out.println(doc);
    for(String queryTerm: queryTerms) {
      Term term = new Term(FIELD_TEXT, queryTerm);
      long termFreqInCorpus2 = 0;
      DocsEnum de = MultiFields.getTermDocsEnum(index, MultiFields.getLiveDocs(index), FIELD_TEXT, new BytesRef(queryTerm));
      int document;
      while((document = de.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
        termFreqInCorpus2 += de.freq();
      }
      long termFreqInCorpus = index.totalTermFreq(term);
      long documentFreq = index.docFreq(term);
      int termFreqInDoc = docTermCountMap.containsKey(queryTerm) ? docTermCountMap.get(queryTerm) : 0;
      score += Math.log((termFreqInDoc + mu*((double)termFreqInCorpus/totalTokens)) 
          / (docLen + mu));
      System.out.println("term: " + queryTerm + " freq in doc: " + termFreqInDoc 
          + " freq in corpus: " + termFreqInCorpus + " freq in corpus2: " + termFreqInCorpus2 );
    }
    return score;
  }
  
  public static List<String> tokenize(TokenStream tokenstream) {
    List<String> output = new ArrayList<String>();
    try {
      tokenstream.reset();
      while (tokenstream.incrementToken()){
        String token = tokenstream.getAttribute(CharTermAttribute.class).toString();
        if (token.length() > 0) {
          output.add(token);
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return output;
  }
  
}
