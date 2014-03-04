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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.tools.bzip2.CBZip2InputStream;

import cc.twittertools.corpus.data.JsonStatusCorpusReader;
import cc.twittertools.corpus.data.Status;
import cc.twittertools.corpus.data.StatusStream;

/**
 * Reference implementation for indexing statuses.
 */
public class IndexStatusesSubcollection {
  private static final Logger LOG = Logger.getLogger(IndexStatusesSubcollection.class);

  public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);
  public static String corpusFormat = null;

  private IndexStatusesSubcollection() {
  }

  public static enum StatusField {
    ID("id"), TEXT("text");

    public final String name;

    StatusField(String s) {
      name = s;
    }
  };

  private static final String HELP_OPTION = "h";
  private static final String COLLECTION_OPTION = "collection";
  private static final String INDEX_OPTION = "index";
  private static final String IDS_OPTION = "ids";
  private static final String OPTIMIZE_OPTION = "optimize";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(HELP_OPTION, "show help"));
    options.addOption(new Option(OPTIMIZE_OPTION, "merge indexes into a single segment"));

    options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("source collection directory").create(COLLECTION_OPTION));
    options.addOption(OptionBuilder.withArgName("dir").hasArg().withDescription("index location")
        .create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("file").hasArg()
        .withDescription("file with deleted tweetids").create(IDS_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (cmdline.hasOption(HELP_OPTION) || !cmdline.hasOption(COLLECTION_OPTION)
        || !cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(IDS_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(IndexStatuses.class.getName(), options);
      System.exit(-1);
    }

    String collectionPath = cmdline.getOptionValue(COLLECTION_OPTION);
    String indexPath = cmdline.getOptionValue(INDEX_OPTION);

    final FieldType textOptions = new FieldType();
    textOptions.setIndexed(true);
    textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    textOptions.setStored(false);
    textOptions.setTokenized(true);

    LOG.info("collection: " + collectionPath);
    LOG.info("index: " + indexPath);

    LongOpenHashSet ids = new LongOpenHashSet();
    File idsFile = new File(cmdline.getOptionValue(IDS_OPTION));
    if (!idsFile.exists()) {
      System.err.println("Error: " + idsFile + " does not exist!");
      System.exit(-1);
    }
    LOG.info("Reading ids from " + idsFile);

    FileInputStream fin = new FileInputStream(idsFile);
    byte[] ignoreBytes = new byte[2];
    fin.read(ignoreBytes); // "B", "Z" bytes from commandline tools
    BufferedReader br = new BufferedReader(new InputStreamReader(new CBZip2InputStream(fin)));

    String s;
    while ((s = br.readLine()) != null) {
      if (s.contains("\t")) {
        ids.add(Long.parseLong(s.split("\t")[0]));
      } else {
        ids.add(Long.parseLong(s));
      }
    }
    br.close();
    fin.close();
    LOG.info("Read " + ids.size() + " tweetids from ids file.");

    long startTime = System.currentTimeMillis();
    File file = new File(collectionPath);
    if (!file.exists()) {
      System.err.println("Error: " + file + " does not exist!");
      System.exit(-1);
    }

    StatusStream stream = new JsonStatusCorpusReader(file);

    Directory dir = FSDirectory.open(new File(indexPath));
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, IndexStatuses.ANALYZER);
    config.setOpenMode(OpenMode.CREATE);

    IndexWriter writer = new IndexWriter(dir, config);
    int cnt = 0;
    Status status;
    try {
      while ((status = stream.next()) != null) {
        if (status.getText() == null) {
          continue;
        }

        if (!ids.contains(status.getId())) {
          continue;
        }

        cnt++;
        Document doc = new Document();
        doc.add(new LongField(StatusField.ID.name, status.getId(), Field.Store.YES));
        doc.add(new Field(StatusField.TEXT.name, status.getText(), textOptions));

        writer.addDocument(doc);
        if (cnt % 100000 == 0) {
          LOG.info(cnt + " statuses indexed");
        }
      }

      LOG.info(String.format("Total of %s statuses added", cnt));

      if (cmdline.hasOption(OPTIMIZE_OPTION)) {
        LOG.info("Merging segments...");
        writer.forceMerge(1);
        LOG.info("Done!");
      }

      LOG.info("Total elapsed time: " + (System.currentTimeMillis() - startTime) + "ms");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      writer.close();
      dir.close();
      stream.close();
    }
  }
}
