package com.twitter.corpus.demo;

import java.io.Reader;

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.ReusableAnalyzerBase;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.util.Version;

public class TweetAnalyzer extends ReusableAnalyzerBase {

	private Version matchVersion;
	/**
	 * Creates a new {@link SimpleAnalyzer}
	 * @param matchVersion Lucene version to match See {@link <a href="#version">above</a>}
	 */
	public TweetAnalyzer(Version matchVersion) {
		this.matchVersion = matchVersion;
	}

	@Override
	protected TokenStreamComponents createComponents(final String fieldName,
			final Reader reader) {
		Tokenizer tok = new ClassicTokenizer(matchVersion, reader);
		TokenFilter filt = new LowerCaseFilter(matchVersion, tok);

		return new TokenStreamComponents(tok, filt);
	}
}
