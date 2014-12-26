package cc.twittertools.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import cc.twittertools.index.TweetAnalyzer;

public class QueryLikelihoodModel {
  private static final String FIELD_TEXT = "text";
  private static final double mu = 2500.0;
  private static IndexReader index;
  private static TweetAnalyzer tokenizer;
  private long totalTokens;
  
  public QueryLikelihoodModel(IndexReader index) throws IOException {
    this.totalTokens = index.getSumTotalTermFreq(FIELD_TEXT);
  }

  //tokenize a term using TweetAnalyzer(stem=true, version=LUCENE_43)
  private String stemTerm(String term) throws IOException {
    TokenStream stream = null;
    stream = tokenizer.tokenStream("text", new StringReader(term));

    CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);
    stream.reset();
    stream.incrementToken();
    String stemTerm = charTermAttribute.toString();
    return stemTerm;
  }

  public Map<String, Float> parseQuery(String query) throws IOException {
    String[] phrases = query.trim().split("[,\\s]+");
    Map<String, Float> weights = new HashMap<String, Float>();
    for(String phrase: phrases) {
      if (phrase.length() == 0) {
        continue;
      }
      
      String tokenizeTerm = null;
      float weight = 0.0f;
      if (phrase.contains("^")) {
        String term = phrase.split("\\^")[0];
        tokenizeTerm = stemTerm(term);
        weight = Float.parseFloat(phrase.split("\\^")[1]);

      } else {
        tokenizeTerm = stemTerm(phrase);
        weight = 1.0f/phrases.length;
      }
      if (weights.containsKey(tokenizeTerm)) {
        weight = weights.get(tokenizeTerm) + weight;
      }
      weights.put(tokenizeTerm, weight);
    }
    System.out.println("weights:"+weights.toString());
    return weights;
  }
  
  public double computeQLScore(Map<String, Float> queryWeights, String doc) throws IOException {
    double score = 0;
    TokenStream docStream = tokenizer.tokenStream(FIELD_TEXT, new StringReader(doc));
    List<String> docTerms = tokenize(docStream);
    //System.out.println("doc:"+docTerms.toString());
    
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

    for(String queryTerm: queryWeights.keySet()) {
      float weight = queryWeights.get(queryTerm);
      Term term = new Term(FIELD_TEXT, queryTerm);
      long termFreqInCorpus = index.totalTermFreq(term);
      if (termFreqInCorpus == 0) continue;
      int termFreqInDoc = docTermCountMap.containsKey(queryTerm) ? docTermCountMap.get(queryTerm) : 0;
      score += weight * Math.log((termFreqInDoc + mu*((double)termFreqInCorpus/totalTokens)) 
          / (docLen + mu));
      //System.out.println("term: " + queryTerm + " freq in doc: " + termFreqInDoc 
      //    + " freq in corpus: " + termFreqInCorpus);
    }
    return score;
  }

  private static List<String> tokenize(TokenStream tokenstream) {
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
