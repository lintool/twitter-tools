package com.twitter.corpus.download;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.twitter.corpus.data.HtmlStatus;

import edu.umd.cloud9.io.pair.PairOfLongString;

/*
 * Original source: https://github.com/spacelis/twitter-corpus-tools
 * Modified slightly by Myle Ott <mott@us.ibm.com>.
 */
public class ExtractEmbeddedJsonHtmlStatusBlock {
    private static final String INPUT_OPTION = "input";
    private static final String OUTPUT_OPTION = "output";

    private static final String JSON_START = "page(";
    private static final String JSON_END = "});";
    private static final JsonParser jsonparser = new JsonParser();
    private static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input crawl SequenceFile").create(INPUT_OPTION));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output gzipped Json objects in lines").create(OUTPUT_OPTION));

        CommandLine cmdline = null;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            System.exit(-1);
        }

        if (!cmdline.hasOption(INPUT_OPTION) && !cmdline.hasOption(OUTPUT_OPTION)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(DumpHtmlStatusCrawl.class.getName(), options);
            System.exit(-1);
        }

        Path inpath = new Path(cmdline.getOptionValue(INPUT_OPTION));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, inpath, conf);
        OutputStreamWriter writer =
            new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(cmdline.getOptionValue(OUTPUT_OPTION))));

        PairOfLongString key = new PairOfLongString();
        HtmlStatus value = new HtmlStatus();

        String html; 
        String json;
        int st, end;
        while (reader.next(key, value)) {
            if (value.getHttpStatusCode() != 200) {
                continue;
            }
            html = value.getHtml();
            st = html.indexOf(JSON_START) ;
            end = html.indexOf(JSON_END, st + JSON_START.length());
            if(st<0 || end<0 || end-st<=0){
                System.err.println(String.format("[WARN] Failed extracting %d %s", key.getLeftElement(), key.getRightElement()));
                continue;
            }
            json = html.substring(st + JSON_START.length(), end + 1).replaceAll("\\r|\\n", "");
            writer.append(json+'\n');
        }
        reader.close();
        writer.close();
    }
}
