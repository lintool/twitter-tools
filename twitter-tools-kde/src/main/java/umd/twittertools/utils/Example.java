package umd.twittertools.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import umd.twittertools.data.TweetSet;
import umd.twittertools.eval.Evaluation;
import umd.twittertools.run.RunTemporalModel;

public class Example {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		RunTemporalModel model = new RunTemporalModel();
		Map<Integer, TweetSet> query2TweetSet = TweetSet.fromFile("data/run.2013.total.txt");
		model.loadGroundTruth("data/qrels.2013.total.txt");
		Integer[] qids = new Integer[query2TweetSet.keySet().size()];
		query2TweetSet.keySet().toArray(qids);
		Arrays.sort(qids);
		double MAP_SUM = 0, P30_SUM = 0;
		int numOfquerys = model.qrels.rowKeySet().size();
		for (int qid: qids) {
			if (!model.qrels.containsRow(qid)) continue;
			double MAP = Evaluation.MAP(qid, query2TweetSet.get(qid), model.qrels, model.numrels);
			double P30 = Evaluation.P_RANK(qid, query2TweetSet.get(qid), model.qrels, 30);
			MAP_SUM += MAP;
			P30_SUM += P30;
			System.out.println(qid + ":" + model.numrels.get(qid) + " " + MAP + " " + P30);
		}
		
		System.out.println("overall: " + MAP_SUM/numOfquerys + " " + P30_SUM/numOfquerys);
	}

}
