package cc.twittertools.model;

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
import org.apache.lucene.util.Version;

import cc.twittertools.index.LowerCaseEntityPreservingFilter;
import cc.twittertools.thrift.gen.TResult;

public class QueryLikelihoodModel {
	
	public static double MU = 100;
	private static Map<String, Integer> corpus; // word2freq map
	private static int corpusTermCounter; // number of terms in corpus
	
	public QueryLikelihoodModel() {}
	
	// Use the returning results as default corpus
	public void setCorpus(List<TResult> queryResults) {
		String contents = "";
		for (TResult result : queryResults) {
			contents += result.getText() + " ";
		}
		corpus = getTermFreqMap(contents);
		for (String term : corpus.keySet()) {
			corpusTermCounter += corpus.get(term);
		}
	}
	
	public List<String> LuceneTokenizer(String input) {
		List<String> output = new ArrayList<String>();
		Tokenizer source = new WhitespaceTokenizer(Version.LUCENE_43, new StringReader((String)input));
    TokenStream tokenstream = new LowerCaseEntityPreservingFilter(source);
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
	
	public Map<String, Integer> getTermFreqMap(String document) {
		Map<String, Integer> tf = new HashMap<String, Integer>();
		List<String> terms = LuceneTokenizer(document);
		
		for (String term: terms) {
			if (tf.containsKey(term)) {
				tf.put(term, tf.get(term)+1);
			} else {
				tf.put(term, 1);
			}
		}
		return tf;
	}
	
	public double computeScore(String query, String doc) {
		double score = 0;
		List<String> queryTerms = LuceneTokenizer(query);
		Map<String, Integer> docTermFreqMap = getTermFreqMap(doc);
		int docTermNum = 0;
		for (String term: docTermFreqMap.keySet()) {
			docTermNum += docTermFreqMap.get(term);
		}
		
		for (String term : queryTerms) {
			int docFreq;
			double corpusScore;
			if (docTermFreqMap.containsKey(term)) {
				docFreq = docTermFreqMap.get(term);
			} else {
				docFreq = 0;
			}
			if (corpus.containsKey(term)) {
				corpusScore = corpus.get(term) * 1.0 / corpusTermCounter;
			} else {
				corpusScore = 1.0 / corpusTermCounter;
			}
			score += Math.log((docFreq + MU * corpusScore) / (docTermNum + MU));
		}
		return score;
	}
	
}
