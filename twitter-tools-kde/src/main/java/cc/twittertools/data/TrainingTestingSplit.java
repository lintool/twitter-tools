package cc.twittertools.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import cc.twittertools.query.TrecTopic;
import cc.twittertools.query.TrecTopicSet;

public class TrainingTestingSplit {
	public static void topicSplit(String file1, String file2) throws Exception {
		TrecTopicSet topic2011 = TrecTopicSet.fromFile(new File(file1));
		TrecTopicSet topic2012 = TrecTopicSet.fromFile(new File(file2));
		TrecTopicSet topics = new TrecTopicSet();
		topics.addAll(topic2011);
		topics.addAll(topic2012);
		TrecTopicSet trainingTopicSet = new TrecTopicSet();
		TrecTopicSet testingTopicSet = new TrecTopicSet();
		for (TrecTopic topic : topics) {
			if (topic.getId() % 2 == 1) {
				trainingTopicSet.add(new TrecTopic(topic));
			} else {
				testingTopicSet.add(new TrecTopic(topic));
			}
		}
		TrecTopicSet.writeBack(new File("data/topics.training.txt"), trainingTopicSet);
		TrecTopicSet.writeBack(new File("data/topics.testing.txt"), testingTopicSet);
	}
	
	public static void qrelsSplit(String file1, String file2) throws Exception {
		List<String> qrels2011 = Files.readLines(new File(file1), Charsets.UTF_8);
		List<String> qrels2012 = Files.readLines(new File(file2), Charsets.UTF_8);
		List<String> qrels = new ArrayList<String>();
		qrels.addAll(qrels2011);
		qrels.addAll(qrels2012);
		BufferedWriter trainingWriter = new BufferedWriter(new FileWriter("data/run.training.txt"));
		BufferedWriter testingWriter = new BufferedWriter(new FileWriter("data/run.testing.txt"));
		for (String line : qrels) {
			String[] groups = line.split("\\s+");
			int topicId = Integer.parseInt(groups[0]);
			if (topicId % 2 == 1) {
				trainingWriter.write(line+"\n");
			} else {
				testingWriter.write(line+"\n");
			}
		}
		
		trainingWriter.close();
		testingWriter.close();
	}
	
	
	
	public static void main(String[] args) throws Exception {
		//topicSplit("../data/topics.microblog2011.txt", "../data/topics.microblog2012.txt");
		qrelsSplit(args[0], args[1]);
	}
}
