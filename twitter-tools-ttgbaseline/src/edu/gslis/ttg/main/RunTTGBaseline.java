package edu.gslis.ttg.main;

import java.io.FileReader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import cc.twittertools.search.api.TrecSearchThriftClient;
import cc.twittertools.thrift.gen.TResult;
import edu.gslis.eval.Qrels;
import edu.gslis.queries.GQueries;
import edu.gslis.queries.GQueriesJsonImpl;
import edu.gslis.queries.GQuery;
import edu.gslis.ttg.clusters.Cluster;
import edu.gslis.ttg.clusters.Clusters;
import edu.gslis.ttg.clusters.clusterers.SimpleJaccardClusterer;
import edu.gslis.ttg.searchers.SimpleSearcher;
import edu.gslis.utils.ParameterBroker;

public class RunTTGBaseline {
	private static final String DEFAULT_RUNTAG = "ttgbaseline";
	
	private static final String HOST_OPTION = "host";
	private static final String TRAINING_PORT = "training_port";
	private static final String TESTING_PORT = "testing_port";
	private static final String QUERIES_OPTION = "queries";
	private static final String NUM_RESULTS_OPTION = "num_results";
	private static final String GROUP_OPTION = "group";
	private static final String TOKEN_OPTION = "token";
	private static final String RUNTAG_OPTION = "runtag";
	private static final String JACCARD_STEP_SIZE = "jaccard_step";
	private static final String TRAINING_QUERIES = "training_queries";
	private static final String TRAINING_CLUSTERS = "training_clusters";
	private static final String QRELS_OPTION = "qrels";
	private static final String EVALUATION_OPTION = "evaluation_type";

	public static void main(String[] args) throws NumberFormatException, TException, UnsupportedEncodingException {
		ParameterBroker params = new ParameterBroker("./config/run_params.json");

		PrintStream out = new PrintStream(System.out, true, "UTF-8");
		PrintStream err = new PrintStream(System.err, true, "UTF-8");
		
		GQueries trainingQueries = new GQueriesJsonImpl();
		trainingQueries.setMetadataField("querytweettime");
		trainingQueries.read(params.getParamValue(TRAINING_QUERIES));
		
		GQueries queries = new GQueriesJsonImpl();
		queries.setMetadataField("querytweettime");
		queries.read(params.getParamValue(QUERIES_OPTION));
		
		Qrels qrels = new Qrels(params.getParamValue(QRELS_OPTION), false, 1);
		
		// max number of docs to send to output
		int numResults = 1000;
		try {
			if (params.getParamValue(NUM_RESULTS_OPTION) != null) {
				numResults = Integer.parseInt(params.getParamValue(NUM_RESULTS_OPTION));
			}
		} catch (NumberFormatException e) {
			err.println("Invalid " + NUM_RESULTS_OPTION + ": " + params.getParamValue(NUM_RESULTS_OPTION));
			System.exit(-1);
		}
		
		// authentication credentials
		String group = params.getParamValue(GROUP_OPTION);
		if(group==null) {
			err.println("Invalid " + GROUP_OPTION + ": must set a valid group ID");
			System.exit(-1);
		}
		String token = params.getParamValue(TOKEN_OPTION);
		if(group==null) {
			err.println("Invalid " + TOKEN_OPTION + ": must set a valid authentication token");
			System.exit(-1);
		}
		
		// ports
		int trainingPort = 9090;
		try {
			if (params.getParamValue(TRAINING_PORT) != null) {
				trainingPort = Integer.parseInt(params.getParamValue(TRAINING_PORT));
			}
		} catch (NumberFormatException e) {
			err.println("Invalid " + TRAINING_PORT + ": " + params.getParamValue(TRAINING_PORT));
			System.exit(-1);
		}
		int testingPort = 9091;
		try {
			if (params.getParamValue(TESTING_PORT) != null) {
				testingPort = Integer.parseInt(params.getParamValue(TESTING_PORT));
			}
		} catch (NumberFormatException e) {
			err.println("Invalid " + TESTING_PORT + ": " + params.getParamValue(TESTING_PORT));
			System.exit(-1);
		}
		
		// run tag
		String runTag = params.getParamValue(RUNTAG_OPTION);
		if(runTag==null) {
			runTag = DEFAULT_RUNTAG;
		}
		
		// jaccard step size
		double stepSize = 0.1;
		try {
			if (params.getParamValue(JACCARD_STEP_SIZE) != null) {
				stepSize = Double.parseDouble(params.getParamValue(JACCARD_STEP_SIZE));
			}
		} catch (NumberFormatException e) {
			err.println("Invalid " + JACCARD_STEP_SIZE + ": " + params.getParamValue(JACCARD_STEP_SIZE));
			System.exit(-1);
		}
		
		// weighted or unweighted evaluation
		String evalType = "weighted";
		try {
			if (params.getParamValue(EVALUATION_OPTION) != null) {
				evalType = params.getParamValue(EVALUATION_OPTION);
			}
		} catch (Exception e) {
			err.println("Invalid " + EVALUATION_OPTION + ": " + params.getParamValue(EVALUATION_OPTION));
			System.exit(-1);
		}
		
		// 2 decimal places
		DecimalFormat df = new DecimalFormat("#.##");
		
		// read in training data
		String trainingFile = params.getParamValue(TRAINING_CLUSTERS);
		if (trainingFile==null) {
			err.println("Invalid " + TRAINING_CLUSTERS + ": please provide valid file.");
			System.exit(-1);
		}
		
		// parse training data into clusters
		Map<String, Clusters> clusterMembership = new HashMap<String, Clusters>();
		JSONParser parser = new JSONParser();
		try {
			JSONObject parseObj = (JSONObject) parser.parse(new FileReader(trainingFile));
			JSONObject topicObj = (JSONObject) parseObj.get("topics");
			Set<String> topics = topicObj.keySet();
			Iterator<String> topicIt = topics.iterator();
			while (topicIt.hasNext()) { // for each topic
				String topic = topicIt.next();
				clusterMembership.put(topic, new Clusters());
				JSONArray clusters = (JSONArray) ((JSONObject) topicObj.get(topic)).get("clusters");
				Iterator<JSONArray> clusterIt = clusters.iterator();
				while (clusterIt.hasNext()) { // for each cluster in the topic
					JSONArray cluster = (JSONArray) clusterIt.next();
					Cluster c = new Cluster();
					Iterator<String> clusterMemberIt = cluster.iterator();
					while (clusterMemberIt.hasNext()) { // for each docId in the cluster
						String member = clusterMemberIt.next();
						long memberId = Long.parseLong(member);
						c.add(memberId);
					}
					clusterMembership.get(topic).add(c);
				}
			}
		} catch (Exception e) {
			err.println("Error reading training data.");
			e.printStackTrace();
			System.exit(-1);
		}
		
		// instantiate search client
		TrecSearchThriftClient client = new TrecSearchThriftClient(params.getParamValue(HOST_OPTION),
				trainingPort, group, token);

		SimpleSearcher searcher = new SimpleSearcher(client, numResults);
		
		err.println("=== Train Queries ===");
		
		List<Double> thresholds = new ArrayList<Double>();
		double averageThreshold = 0;
		Iterator<GQuery> queryIterator = trainingQueries.iterator();
		while(queryIterator.hasNext()) {
			GQuery query = queryIterator.next();
			
			Map<Long, TResult> seenResults = searcher.search(query);
			
			SimpleJaccardClusterer clusterer = new SimpleJaccardClusterer(new ArrayList<TResult>(seenResults.values()));
			
			// sweep through jaccard steps, calculating F1
			double maxF1 = 0;
			double maxF1Threshold = 1;
			for (double j = 1.0; j >= 0.0; j -= stepSize) { // for each jaccard threshold step
				Clusters clusters = clusterer.cluster(j);
				
				// all clusters are created now, get a finalized set of results
				Set<Long> allResults = new HashSet<Long>(seenResults.keySet());
				allResults.removeAll(clusters.getAllClusteredResults()); // allResults includes unclustered plus one representative from each cluster
				for (Cluster c : clusters) {
					allResults.add(c.getFirstMember());
				}
				
				// calculate f1 on the finalized set
				Clusters seenClusters = new Clusters();
				Clusters trueClusters = clusterMembership.get(query.getTitle());
				Iterator<Long> resultIt = allResults.iterator();
				while (resultIt.hasNext()) {
					long result = resultIt.next();
					Cluster trueCluster = trueClusters.findCluster(result);
					if (trueCluster != null) { // if it is relevant, it will have a true cluster; if this is null, it's non-relevant
						seenClusters.add(trueCluster);
					}
				}
				
				int numRetrievedClusters = seenClusters.size();
				int numResultsReturned = allResults.size();
				int numTrueClusters = trueClusters.size();

				double precision = 0;
				double recall = 0;
				double f1 = 0;
				if (evalType.equals("unweighted")) {
					precision = numRetrievedClusters / (double) numResultsReturned;
					recall = numRetrievedClusters / (double) numTrueClusters;
					f1 = 2 * precision * recall / (precision + recall);
				} else {				
					// for weighted measurements, we need the weight of each cluster
					int retrievedWeight = 0;
					for (Cluster cluster : seenClusters) {
						int w = cluster.getWeight(query, qrels);
						retrievedWeight += w;
					}
					int resultsWeight = 0;
					for (long result : allResults) {
						int w = 0;
						if (seenClusters.findCluster(result) == null)
						resultsWeight += w;
					}
					int trueWeight = 0;
					for (Cluster cluster : trueClusters) {
						int w = cluster.getWeight(query, qrels);
						trueWeight += w;
					}
					
					precision = retrievedWeight / (double) resultsWeight; // <--- ??????
					recall = retrievedWeight / (double) trueWeight;
					f1 = 2 * precision * recall / (precision + recall);
				}
				if (f1 > maxF1) {
					maxF1 = f1;
					maxF1Threshold = j;
				}
			}
			thresholds.add(maxF1Threshold);
			err.println("F1: "+df.format(maxF1)+"; Jaccard: "+df.format(maxF1Threshold));
			
		}
		
		// get the average threshold
		for (double threshold : thresholds) {
			averageThreshold += threshold;
		}
		averageThreshold /= thresholds.size();
		err.println("Average Jaccard: "+averageThreshold);
		
		err.println("=== Test Queries ===");
		
		// now cluster the test queries and output
		queryIterator = queries.iterator();
		while(queryIterator.hasNext()) {
			GQuery query = queryIterator.next();
			err.println(query.getTitle());
			
			client = new TrecSearchThriftClient(params.getParamValue(HOST_OPTION), testingPort, group, token);
			searcher = new SimpleSearcher(client, numResults);
			Map<Long, TResult> seenResults = searcher.search(query);
			
			SimpleJaccardClusterer clusterer = new SimpleJaccardClusterer(new ArrayList<TResult>(seenResults.values()));
			Clusters clusters = clusterer.cluster(averageThreshold);
			
			// all clusters are created now, get a finalized set of results
			Set<Long> allResults = new HashSet<Long>(seenResults.keySet());
			allResults.removeAll(clusters.getAllClusteredResults()); // allResults includes unclustered plus one representative from each cluster
			for (Cluster c : clusters) {
				allResults.add(c.getFirstMember());
			}
			
			int i = 0;
			for (long result : allResults) {
				TResult hit = seenResults.get(result);
				out.println(String.format("%s Q0 %s %d %f %s", query.getTitle(), hit.getId(), i, hit.getRsv(), runTag));
				i++;
			}
		}
		out.close();
		err.close();
	}

}
