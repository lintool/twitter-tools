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

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.util.Version;

import com.google.common.base.Preconditions;

public final class TweetAnalyzer extends Analyzer {
  private final Version matchVersion;
  private final boolean stemming;

  public TweetAnalyzer(Version matchVersion, boolean stemming) {
    this.matchVersion = Preconditions.checkNotNull(matchVersion);
    this.stemming = stemming;
  }

  public TweetAnalyzer(Version matchVersion) {
    this(matchVersion, true);
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
    Tokenizer source = new WhitespaceTokenizer(matchVersion, reader);
    TokenStream filter = new LowerCaseEntityPreservingFilter(source);

    if (stemming) {
      // Porter stemmer ignores words which are marked as keywords
      filter = new PorterStemFilter(filter);
    }
    return new TokenStreamComponents(source, filter);
  }

}