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

import cc.twittertools.corpus.data.JsonStatusCorpusReader;
import cc.twittertools.corpus.data.Status;
import cc.twittertools.corpus.data.StatusStream;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class ExtractSubcollectionUsers {
  private static final Logger LOG = Logger.getLogger(ExtractSubcollectionUsers.class);

  private static final String COLLECTION_OPTION = "collection";
  private static final String ID_OPTION = "userids";
  private static final String OUTPUT_OPTION = "output";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("source collection directory").create(COLLECTION_OPTION));
    options.addOption(OptionBuilder.withArgName("file").hasArg()
        .withDescription("list of userids").create(ID_OPTION));
    options.addOption(OptionBuilder.withArgName("file").hasArg()
        .withDescription("output JSON").create(OUTPUT_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(COLLECTION_OPTION) || !cmdline.hasOption(ID_OPTION) ||
        !cmdline.hasOption(OUTPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(ExtractSubcollection.class.getName(), options);
      System.exit(-1);
    }

    String outputFile = cmdline.getOptionValue(OUTPUT_OPTION);
    String collectionPath = cmdline.getOptionValue(COLLECTION_OPTION);

    LongOpenHashSet userids = new LongOpenHashSet();
    File useridsFile = new File(cmdline.getOptionValue(ID_OPTION));
    if (!useridsFile.exists()) {
      System.err.println("Error: " + useridsFile + " does not exist!");
      System.exit(-1);
    }
    LOG.info("Reading userids from " + useridsFile);

    FileInputStream fin = new FileInputStream(useridsFile);
    BufferedReader br = new BufferedReader(new InputStreamReader(fin));

    String s;
    while ((s = br.readLine()) != null) {
      userids.add(Long.parseLong(s));
    }
    br.close();
    fin.close();
    LOG.info("Read " + userids.size() + " userids.");

    File file = new File(collectionPath);
    if (!file.exists()) {
      System.err.println("Error: " + file + " does not exist!");
      System.exit(-1);
    }

    Writer out = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(outputFile), "UTF-8"));

    int cnt = 0;
    long total = 0;
    StatusStream stream = new JsonStatusCorpusReader(file);
    Status status;
    while ((status = stream.next()) != null) {
      total++;
      if (total % 1000000 == 0) {
        LOG.info(total + " tweets processed");
      }
      if (userids.contains(status.getUserId())) {
        cnt++;
        LOG.info(status.getScreenname() + "\t" + status.getText().replaceAll("[\\n\\r]+", " "));
        out.write(status.getJsonObject().toString() + "\n");
      }
    }
    stream.close();
    out.close();

    LOG.info("Processed " + total + " tweets.");
    LOG.info("Extracted " + cnt + " tweets.");

    LOG.info("Done!");
  }
}
