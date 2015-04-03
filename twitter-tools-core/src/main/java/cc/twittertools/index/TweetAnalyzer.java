/**
 * Twitter Tools
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cc.twittertools.index;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public final class TweetAnalyzer extends Analyzer {
  private final Version matchVersion;
  private final boolean stemming;
  private final Set<String> stopwords;

  public TweetAnalyzer(Version matchVersion, boolean stemming) {
    this(matchVersion, stemming, null);
  }

  public TweetAnalyzer(Version matchVersion) {
    this(matchVersion, true, null);
  }

  public TweetAnalyzer() {
    this(Version.LUCENE_43, true, null);
  }

  public TweetAnalyzer(Version matchVersion, boolean stemming, Set<String> stopwords) {
    this.matchVersion = Preconditions.checkNotNull(matchVersion);
    this.stemming = stemming;
    this.stopwords = stopwords;
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
    Tokenizer source = new WhitespaceTokenizer(matchVersion, reader);
    TokenStream filter = new LowerCaseEntityPreservingFilter(source);

    if (stopwords != null) {
      // stop words
      CharArraySet charArraySet = new CharArraySet(matchVersion, stopwords, true);
      filter = new StopFilter(matchVersion, filter, charArraySet);
    }
    if (stemming) {
      // Porter stemmer ignores words which are marked as keywords
      filter = new PorterStemFilter(filter);
    }
    return new TokenStreamComponents(source, filter);
  }

  public List<String> tokenize(String tweet) throws IOException {
    List<String> list = Lists.newArrayList();

    TokenStream tokenStream = this.tokenStream(null, new StringReader(tweet));
    CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
    tokenStream.reset();
    while (tokenStream.incrementToken()) {
      if (cattr.toString().length() == 0) {
        continue;
      }
      list.add(cattr.toString());
    }
    tokenStream.end();
    tokenStream.close();

    return list;
  }
}