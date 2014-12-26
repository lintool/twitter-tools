package umd.twittertools.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import umd.twittertools.data.TrecTopic;
import umd.twittertools.data.TrecTopicSet;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TrainingTestingSplit {
	public static void topicSplit(String file1, String file2) throws Exception {
		TrecTopicSet topic2011 = TrecTopicSet.fromFile(new File(file1));
		TrecTopicSet topic2012 = TrecTopicSet.fromFile(new File(file2));
		TrecTopicSet topics = new TrecTopicSet();
		topics.addAll(topic2011);
		topics.addAll(topic2012);
		//TrecTopicSet topics = TrecTopicSet.fromFile(new File(topicFile));
		TrecTopicSet trainingTopicSet = new TrecTopicSet();
		TrecTopicSet testingTopicSet = new TrecTopicSet();
		for (TrecTopic topic : topics) {
			if (topic.getId() % 2 == 0) {
				trainingTopicSet.add(new TrecTopic(topic));
			} else {
				testingTopicSet.add(new TrecTopic(topic));
			}
		}
		TrecTopicSet.writeBack(new File("data/topics.training.txt"), trainingTopicSet);
		TrecTopicSet.writeBack(new File("data/topics.testing.txt"), testingTopicSet);
	}
	
	public static void qrelsSplit(String file) throws Exception {
		List<String> qrels = Files.readLines(new File(file), Charsets.UTF_8);
		BufferedWriter trainingWriter = new BufferedWriter(new FileWriter("data/run.2013.training.txt"));
		BufferedWriter testingWriter = new BufferedWriter(new FileWriter("data/run.2013.testing.txt"));
		for (String line : qrels) {
			String[] groups = line.split("\\s+");
			int topicId = Integer.parseInt(groups[0]);
			if (topicId % 2 == 0) {
				trainingWriter.write(line+"\n");
			} else {
				testingWriter.write(line+"\n");
			}
		}
		
		trainingWriter.close();
		testingWriter.close();
	}
	
	
	
	public static void main(String[] args) throws Exception {
		//qrelsSplit("../data/qrels.microblog2013.txt", "../data/qrels.microblog2014.txt");
		qrelsSplit(args[0]);
	}
}
