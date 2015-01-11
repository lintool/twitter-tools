package umd.twittertools.model;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import umd.twittertools.data.TweetSet;
import umd.twittertools.model.KDEModel.WeightEnum;

import com.google.common.base.Charsets;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.io.Files;

public class Model {
	public static final int TIME_INTERVAL = 60*60*24;
	public static boolean debug = true;
	public static boolean per_query = true;
	public static int numOfquerys;
	public static String outputDir = "./";
	
	public void computeTMScore(TweetSet tweetSet, double lambda){}
	
	public void computeTMScore(TweetSet tweetSet, TweetSet oracleSet, WeightEnum woption, double lambda){}
	
	public void train(Map<Integer, TweetSet> query2TweetSet, Table<Integer, Long, Integer> qrels
			, Map<Integer, Integer> numrels) throws Exception{}
	
	public void test(Map<Integer, TweetSet> query2TweetSet, Table<Integer, Long, Integer> qrels
			, Map<Integer, Integer> numrels) throws Exception{}
	
	public double getMAP(){ return 0;}
	public double[] getMAParray(){ return new double[]{0, 0, 0, 0, 0}; }
	public double getP30(){ return 0;}
	public double[] getP30array(){ return new double[]{0, 0, 0, 0, 0}; }
	
}
