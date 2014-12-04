package umd.twittertools.run;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import umd.twittertools.data.TweetSet;
import umd.twittertools.eval.Evaluation;
import umd.twittertools.model.KDEModel;
import umd.twittertools.model.Model;
import umd.twittertools.model.RecencyModel;
import umd.twittertools.model.WINModel;
import umd.twittertools.model.KDEModel.WeightEnum;
import umd.twittertools.utils.RandomTest;
import umontreal.iro.lecuyer.util.PrintfFormat;
import cc.twittertools.search.api.RunQueriesThrift;

import com.google.common.base.Charsets;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.io.Files;

public class RunTemporalModel {
	
	private static Model model;
	// query id, tweet id -> relevance score
	public static Table<Integer, Long, Integer> qrels;
	// query id -> number of relevant results 
	public static Map<Integer, Integer> numrels;
	// query id -> top 10 relevant tweets
	
	private static final String MODEL_OPTION = "model";
  private static final String TRAIN_INPUT_OPTION = "traininput";
  private static final String TRAIN_QRELS_OPTION = "trainqrels";
  private static final String TEST_INPUT_OPTION = "testinput";
  private static final String TEST_QRELS_OPTION = "testqrels";
  private static final String OUTPUT_DIR_OPTION = "output";
  private static final String DEBUG_OPTION = "debug";
	 
	// load ground truth qrels
	public static Table<Integer, Long, Integer> loadGroundTruth(String fileAddr) throws IOException {
		qrels = HashBasedTable.create();
		numrels = new HashMap<Integer, Integer>();
		List<String> lines = Files.readLines(new File(fileAddr), Charsets.UTF_8);
		int counter = 0;
		int prevQid = -1;
		for(String line: lines) {
			String groups[] = line.split("\\s+");
			int qid = Integer.parseInt(groups[0]); // query id
			long tweetid = Long.parseLong(groups[2]); // tweet id;
			int score = Integer.parseInt(groups[3]); // positive means relevant, otherwise irrelevant
			if (score > 0) {
				counter += 1; 
				qrels.put(qid, tweetid, score);
			}
			if (qid != prevQid) {
				numrels.put(prevQid, counter);
				counter = 0;
			}
			prevQid = qid;
		}
		numrels.put(prevQid, counter);
		counter = 0;
		return qrels;
	}
	
	public void train(String searchResultFile, String qrelsFile) throws Exception {
		Map<Integer, TweetSet> query2TweetSet = TweetSet.fromFile(searchResultFile);
		loadGroundTruth(qrelsFile);
		model.numOfquerys = qrels.rowKeySet().size();
		model.train(query2TweetSet, qrels, numrels);
	}
	
	public void test(String searchResultFile, String qrelsFile) throws Exception {
		Map<Integer, TweetSet> query2TweetSet = TweetSet.fromFile(searchResultFile);
		loadGroundTruth(qrelsFile);
		model.numOfquerys = qrels.rowKeySet().size();
		model.test(query2TweetSet, qrels, numrels);
	}
	
	// randomize topics for train/test topic split
	public void randomTest(String searchResultFile, String qrelsFile) throws Exception {
		Map<Integer, TweetSet> query2TweetSet = TweetSet.fromFile(searchResultFile);
		loadGroundTruth(qrelsFile);
		RandomTest.randomTest(query2TweetSet);
	}
	
	public static void main(String[] args) throws Exception {
		Options options = new Options();

    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("model").create(MODEL_OPTION));
    options.addOption(OptionBuilder.withArgName("train").hasArg()
        .withDescription("train input").create(TRAIN_INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("train").hasArg()
        .withDescription("train qrels").create(TRAIN_QRELS_OPTION));
    options.addOption(OptionBuilder.withArgName("test").hasArg()
        .withDescription("test input").create(TEST_INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("test").hasArg()
        .withDescription("test qrels").create(TEST_QRELS_OPTION));
    options.addOption(OptionBuilder.withArgName("output").hasArg()
        .withDescription("output dir").create(OUTPUT_DIR_OPTION));
    options.addOption(OptionBuilder.withArgName("debug").hasArg()
        .withDescription("debug option").create(DEBUG_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(MODEL_OPTION) || !cmdline.hasOption(TRAIN_INPUT_OPTION)
        || !cmdline.hasOption(TRAIN_QRELS_OPTION) || !cmdline.hasOption(TEST_INPUT_OPTION)
        || !cmdline.hasOption(TEST_QRELS_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(RunTemporalModel.class.getName(), options);
      System.exit(-1);
    }
    
    String modelOption = cmdline.getOptionValue(MODEL_OPTION);
    if (modelOption.equals("kde")) {
    	model = new KDEModel();
    } else if (modelOption.equals("win")) {
    	model = new WINModel();
    } else if (modelOption.equals("recency")) {
    	model = new RecencyModel();
    } else {
    	System.err.println("Error model name: " + modelOption);
      System.exit(-1);
    }
    
    if (cmdline.hasOption(OUTPUT_DIR_OPTION)) {
    	model.outputDir = cmdline.getOptionValue(OUTPUT_DIR_OPTION);
    }
    if (cmdline.hasOption(MODEL_OPTION)) {
    	model.debug = Boolean.parseBoolean(cmdline.getOptionValue(DEBUG_OPTION));
    }
    
    String trainInputFile = cmdline.getOptionValue(TRAIN_INPUT_OPTION);
    String trainQrelsFile = cmdline.getOptionValue(TRAIN_QRELS_OPTION);
    String testInputFile = cmdline.getOptionValue(TEST_INPUT_OPTION);
    String testQrelsFile = cmdline.getOptionValue(TEST_QRELS_OPTION);
    
    RunTemporalModel instance = new RunTemporalModel();
		instance.train(trainInputFile, trainQrelsFile);
		instance.test(testInputFile, testQrelsFile);
		//instance.randomTest(trainInputFile, trainQrelsFile);
	}
}
