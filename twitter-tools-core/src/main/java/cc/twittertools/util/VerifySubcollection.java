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

package cc.twittertools.util;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import cc.twittertools.corpus.data.JsonStatusCorpusReader;
import cc.twittertools.corpus.data.Status;
import cc.twittertools.corpus.data.StatusStream;

import com.google.common.collect.Maps;

public class VerifySubcollection {
  private static final Logger LOG = Logger.getLogger(VerifySubcollection.class);

  private static final String COLLECTION_OPTION = "collection";
  private static final String ID_OPTION = "tweetids";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("source collection directory").create(COLLECTION_OPTION));
    options.addOption(OptionBuilder.withArgName("file").hasArg()
        .withDescription("list of tweetids").create(ID_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(COLLECTION_OPTION) || !cmdline.hasOption(ID_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(ExtractSubcollection.class.getName(), options);
      System.exit(-1);
    }

    String collectionPath = cmdline.getOptionValue(COLLECTION_OPTION);

    LongOpenHashSet tweetids = new LongOpenHashSet();
    File tweetidsFile = new File(cmdline.getOptionValue(ID_OPTION));
    if (!tweetidsFile.exists()) {
      System.err.println("Error: " + tweetidsFile + " does not exist!");
      System.exit(-1);
    }
    LOG.info("Reading tweetids from " + tweetidsFile);

    FileInputStream fin = new FileInputStream(tweetidsFile);
    BufferedReader br = new BufferedReader(new InputStreamReader(fin));

    String s;
    while ((s = br.readLine()) != null) {
      tweetids.add(Long.parseLong(s));
    }
    br.close();
    fin.close();
    LOG.info("Read " + tweetids.size() + " tweetids.");

    File file = new File(collectionPath);
    if (!file.exists()) {
      System.err.println("Error: " + file + " does not exist!");
      System.exit(-1);
    }

    LongOpenHashSet seen = new LongOpenHashSet();
    TreeMap<Long, String> tweets = Maps.newTreeMap();

    PrintStream out = new PrintStream(System.out, true, "UTF-8");
    StatusStream stream = new JsonStatusCorpusReader(file);
    Status status;
    int cnt = 0;
    while ((status = stream.next()) != null) {
      if (!tweetids.contains(status.getId())) {
        LOG.error("tweetid " + status.getId() + " doesn't belong in collection");
        continue;
      }
      if (seen.contains(status.getId())) {
        LOG.error("tweetid " + status.getId() + " already seen!");
        continue;
      }

      tweets.put(status.getId(), status.getJsonObject().toString());
      seen.add(status.getId());
      cnt++;
    }
    LOG.info("total of " + cnt + " tweets in subcollection.");

    for ( Map.Entry<Long, String> entry : tweets.entrySet()){
      out.println(entry.getValue());
    }

    stream.close();
    out.close();
  }
}
