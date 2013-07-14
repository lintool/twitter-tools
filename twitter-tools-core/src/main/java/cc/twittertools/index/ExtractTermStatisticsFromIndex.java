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

import java.io.File;
import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import cc.twittertools.index.IndexStatuses.StatusField;

public class ExtractTermStatisticsFromIndex {
  private static final String INDEX_OPTION = "index";
  private static final String MIN_OPTION = "min";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("index").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("min").create(MIN_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INDEX_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(ExtractTermStatisticsFromIndex.class.getName(), options);
      System.exit(-1);
    }

    String indexLocation = cmdline.getOptionValue(INDEX_OPTION);
    int min = cmdline.hasOption(MIN_OPTION) ?
        Integer.parseInt(cmdline.getOptionValue(MIN_OPTION)) : 1;

    PrintStream out = new PrintStream(System.out, true, "UTF-8");

    IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(indexLocation)));
    Terms terms = SlowCompositeReaderWrapper.wrap(reader).terms(StatusField.TEXT.name);
    TermsEnum termsEnum = terms.iterator(TermsEnum.EMPTY);

    long missingCnt = 0;
    int skippedTerms = 0;
    BytesRef bytes = new BytesRef();
    while ( (bytes = termsEnum.next()) != null) {
      byte[] buf = new byte[bytes.length];
      System.arraycopy(bytes.bytes, 0, buf, 0, bytes.length);
      String term = new String(buf, "UTF-8");
      int df = termsEnum.docFreq();
      long cf = termsEnum.totalTermFreq();

      if ( df < min) {
        skippedTerms++;
        missingCnt += cf;
        continue;
      }

      out.println(term + "\t" + df + "\t" + cf);
    }

    reader.close();
    out.close();
    System.err.println("skipped terms: " + skippedTerms + ", cnt: " + missingCnt);
  }
}
