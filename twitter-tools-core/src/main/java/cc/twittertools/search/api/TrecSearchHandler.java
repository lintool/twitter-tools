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

package cc.twittertools.search.api;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.log4j.Logger;
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
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.index.IndexStatuses.StatusField;
import cc.twittertools.thrift.gen.TQuery;
import cc.twittertools.thrift.gen.TResult;
import cc.twittertools.thrift.gen.TrecSearch;
import cc.twittertools.thrift.gen.TrecSearchException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class TrecSearchHandler implements TrecSearch.Iface {
  private static final Logger LOG = Logger.getLogger(TrecSearchHandler.class);

  private static QueryParser QUERY_PARSER =
      new QueryParser(Version.LUCENE_43, StatusField.TEXT.name, IndexStatuses.ANALYZER);

  private final IndexSearcher searcher;
  private final Map<String, String> credentials;

  public TrecSearchHandler(File indexPath, @Nullable Map<String, String> credentials)
      throws IOException {
    Preconditions.checkNotNull(indexPath);
    Preconditions.checkArgument(indexPath.exists());

    // Can be null, in which case we don't check for credentials.
    this.credentials = credentials;

    IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath));
    searcher = new IndexSearcher(reader);
    searcher.setSimilarity(new LMDirichletSimilarity(2500.0f));
  }

  public List<TResult> search(TQuery query) throws TrecSearchException {
    Preconditions.checkNotNull(query);

    LOG.info(String.format("Incoming request (%s, %s)", query.group, query.token));

    // Verify credentials.
    if (credentials != null && (!credentials.containsKey(query.group) ||
        !credentials.get(query.group).equals(query.token))) {
      LOG.info(String.format("Access denied for (%s, %s)", query.group, query.token));
      throw new TrecSearchException("Invalid credentials: access denied.");
    }

    List<TResult> results = Lists.newArrayList();
    long startTime = System.currentTimeMillis();

    try {
      Filter filter =
          NumericRangeFilter.newLongRange(StatusField.ID.name, 0L, query.max_id, true, true);

      Query q = QUERY_PARSER.parse(query.text);
      int num = query.num_results > 10000 ? 10000 : query.num_results;
      TopDocs rs = searcher.search(q, filter, num);
      for (ScoreDoc scoreDoc : rs.scoreDocs) {
        Document hit = searcher.doc(scoreDoc.doc);

        TResult p = new TResult();
        p.id = (Long) hit.getField(StatusField.ID.name).numericValue();
        p.screen_name = hit.get(StatusField.SCREEN_NAME.name);
        p.epoch = (Long) hit.getField(StatusField.EPOCH.name).numericValue();
        p.text = hit.get(StatusField.TEXT.name);
        p.rsv = scoreDoc.score;

        p.followers_count = (Integer) hit.getField(StatusField.FOLLOWERS_COUNT.name).numericValue();
        p.statuses_count = (Integer) hit.getField(StatusField.STATUSES_COUNT.name).numericValue();

        if ( hit.get(StatusField.LANG.name) != null) {
          p.lang = hit.get(StatusField.LANG.name);
        }

        if ( hit.get(StatusField.IN_REPLY_TO_STATUS_ID.name) != null) {
          p.in_reply_to_status_id = (Long) hit.getField(StatusField.IN_REPLY_TO_STATUS_ID.name).numericValue();
        }

        if ( hit.get(StatusField.IN_REPLY_TO_USER_ID.name) != null) {
          p.in_reply_to_user_id = (Long) hit.getField(StatusField.IN_REPLY_TO_USER_ID.name).numericValue();
        }

        if ( hit.get(StatusField.RETWEETED_STATUS_ID.name) != null) {
          p.retweeted_status_id = (Long) hit.getField(StatusField.RETWEETED_STATUS_ID.name).numericValue();
        }

        if ( hit.get(StatusField.RETWEETED_USER_ID.name) != null) {
          p.retweeted_user_id = (Long) hit.getField(StatusField.RETWEETED_USER_ID.name).numericValue();
        }

        if ( hit.get(StatusField.RETWEET_COUNT.name) != null) {
          p.retweeted_count = (Integer) hit.getField(StatusField.RETWEET_COUNT.name).numericValue();
        }

        results.add(p);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new TrecSearchException(e.getMessage());
    }

    long endTime = System.currentTimeMillis();
    LOG.info(String.format("%4dms %s", (endTime - startTime), query.toString()));

    return results;
  }
}