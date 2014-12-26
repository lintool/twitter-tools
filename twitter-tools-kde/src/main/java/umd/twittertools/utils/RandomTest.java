package umd.twittertools.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import umd.twittertools.data.TweetSet;
import umd.twittertools.model.KDEModel;
import umd.twittertools.model.RecencyModel;
import umd.twittertools.model.WINModel;
import umd.twittertools.run.RunTemporalModel;

import com.google.common.collect.Table;

public class RandomTest {
	
	public static void randomTest(Map<Integer, TweetSet> query2TweetSet) throws Exception {
		Table<Integer, Long, Integer> qrels = RunTemporalModel.qrels;
		Map<Integer, Integer> numrels = RunTemporalModel.numrels;
		List<Integer> topics = new ArrayList<Integer>();
		topics.addAll(query2TweetSet.keySet());
		KDEModel kdeModel = new KDEModel();
		WINModel winModel = new WINModel();
		RecencyModel recencyModel = new RecencyModel();
		
		Map<Integer, TweetSet> trainTweetSet, testTweetSet;
		
		int repeat = 50;
		double KDE_MAP[][] = new double[repeat][5];
		double KDE_P30[][] = new double[repeat][5];
		double WIN_MAP[] = new double[repeat];
		double WIN_P30[] = new double[repeat];
		double RECENCY_MAP[] = new double[repeat];
		double RECENCY_P30[] = new double[repeat];
		
		int numOfTopics = topics.size();
		for (int iter = 0; iter < repeat; iter++) {
			System.out.println("iteration " + iter + ".");
			Collections.shuffle(topics);
			// split topics
			int evenTopics = 0, oddTopics = 0;
			trainTweetSet = new HashMap<Integer, TweetSet>();
			testTweetSet = new HashMap<Integer, TweetSet>();
			for(int index = 0; index < topics.size(); index++) {
				int topic = topics.get(index);
				if (index % 2 == 0) {
					if (qrels.containsRow(topic)) evenTopics++;
					trainTweetSet.put(topic, query2TweetSet.get(topic));
				} else {
					if (qrels.containsRow(topic)) oddTopics++;
					testTweetSet.put(topic, query2TweetSet.get(topic));
				}
			}
			
			kdeModel.numOfquerys = evenTopics;
			kdeModel.train(trainTweetSet, qrels, numrels);
			kdeModel.numOfquerys = oddTopics;
			kdeModel.test(testTweetSet, qrels, numrels);
			System.arraycopy(kdeModel.getMAParray(), 0, KDE_MAP[iter], 0, 5);
			System.arraycopy(kdeModel.getP30array(), 0, KDE_P30[iter], 0, 5);
			
			recencyModel.numOfquerys = evenTopics;
			recencyModel.train(trainTweetSet, qrels, numrels);
			recencyModel.numOfquerys = oddTopics;
			recencyModel.test(testTweetSet, qrels, numrels);
			RECENCY_MAP[iter] = recencyModel.getMAP();
			RECENCY_P30[iter] = recencyModel.getP30();
			
			winModel.numOfquerys = evenTopics;
			winModel.train(trainTweetSet, qrels, numrels);
			winModel.numOfquerys = oddTopics;
			winModel.test(testTweetSet, qrels, numrels);
			WIN_MAP[iter] = winModel.getMAP();
			WIN_P30[iter] = winModel.getP30();
		}
		
		BufferedWriter bw = new BufferedWriter(new FileWriter("output/random_test_map.txt"));
		bw.write("recency win uniform score rank oracle\n");
		for (int iter = 0; iter < repeat; iter++) {
			bw.write(String.format("%.4f %.4f %.4f %.4f %.4f %.4f\n", 
					RECENCY_MAP[iter] - KDE_MAP[iter][4],
					WIN_MAP[iter] - KDE_MAP[iter][4],
					KDE_MAP[iter][0] - KDE_MAP[iter][4],
					KDE_MAP[iter][1] - KDE_MAP[iter][4],
					KDE_MAP[iter][2] - KDE_MAP[iter][4],
					KDE_MAP[iter][3] - KDE_MAP[iter][4]));
		}
		bw.close();
		
	  bw = new BufferedWriter(new FileWriter("output/random_test_p30.txt"));
		bw.write("recency win uniform score rank oracle\n");
		for (int iter = 0; iter < repeat; iter++) {
			bw.write(String.format("%.4f %.4f %.4f %.4f %.4f %.4f\n", 
					RECENCY_P30[iter] - KDE_P30[iter][4],
					WIN_P30[iter] - KDE_P30[iter][4],
					KDE_P30[iter][0] - KDE_P30[iter][4],
					KDE_P30[iter][1] - KDE_P30[iter][4],
					KDE_P30[iter][2] - KDE_P30[iter][4],
					KDE_P30[iter][3] - KDE_P30[iter][4]));
		}
		bw.close();
	}
}
