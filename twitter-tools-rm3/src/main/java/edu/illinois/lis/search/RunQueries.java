package edu.illinois.lis.search;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;






import cc.twittertools.search.api.TrecSearchThriftClient;
import cc.twittertools.thrift.gen.TResult;
import edu.illinois.lis.document.FeatureVector;
import edu.illinois.lis.feedback.FeedbackRelevanceModel;
import edu.illinois.lis.query.GQueries;
import edu.illinois.lis.query.GQueriesJsonImpl;
import edu.illinois.lis.query.GQuery;
import edu.illinois.lis.utils.ParameterBroker;
import edu.illinois.lis.utils.Stopper;

public class RunQueries {
	private static final String DEFAULT_RUNTAG = "lucene4lm";

	private static final String HOST_OPTION = "host";
	private static final String PORT_OPTION = "port";
	private static final String QUERIES_OPTION = "queries";
	private static final String STOPPER_OPTION = "stopper";
	private static final String FB_DOCS_OPTION = "fb_docs";
	private static final String FB_TERMS_OPTION = "fb_terms";
	private static final String NUM_RESULTS_OPTION = "num_results";
	private static final String GROUP_OPTION = "group";
	private static final String TOKEN_OPTION = "token";
	private static final String RUNTAG_OPTION = "runtag";

	private static final double ORIG_QUERY_WEIGHT = 0.5;
	
	private RunQueries() {}

	public static void main(String[] args) throws Exception {
		ParameterBroker params = new ParameterBroker(args[0]);

		PrintStream out = new PrintStream(System.out, true, "UTF-8");
		PrintStream err = new PrintStream(System.err, true, "UTF-8");

		GQueries queries = new GQueriesJsonImpl();
		queries.read(params.getParamValue(QUERIES_OPTION));
		
		Stopper stopper = null;
		if(params.getParamValue(STOPPER_OPTION) != null)
			stopper = new Stopper(params.getParamValue(STOPPER_OPTION));
		
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

		int fbDocs = 0;
		try {
			if (params.getParamValue(FB_DOCS_OPTION) != null) {
				fbDocs = Integer.parseInt(params.getParamValue(FB_DOCS_OPTION));
			}
		} catch (NumberFormatException e) {
			err.println("Invalid " + FB_DOCS_OPTION + ": " + params.getParamValue(FB_DOCS_OPTION));
			System.exit(-1);
		}
		
		int fbTerms = 0;
		try {
			if (params.getParamValue(FB_TERMS_OPTION) != null) {
				fbTerms = Integer.parseInt(params.getParamValue(FB_TERMS_OPTION));
			}
		} catch (NumberFormatException e) {
			err.println("Invalid " + FB_TERMS_OPTION + ": " + params.getParamValue(FB_TERMS_OPTION));
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

		TrecSearchThriftClient client = new TrecSearchThriftClient(params.getParamValue(HOST_OPTION),
				Integer.parseInt(params.getParamValue(PORT_OPTION)), group, token);

		Iterator<GQuery> queryIterator = queries.iterator();
		while(queryIterator.hasNext()) {
			GQuery query = queryIterator.next();
			System.err.println(query.getTitle());
			String queryText = query.getText();
			
			// stupid hack.  need to lowercase the query vector
			FeatureVector temp = new FeatureVector(null);
			Iterator<String> qTerms = query.getFeatureVector().iterator();
			while(qTerms.hasNext()) {
				String term = qTerms.next();
				temp.addTerm(term.toLowerCase(), query.getFeatureVector().getFeaturetWeight(term));
			}
			temp.normalizeToOne();
			query.setFeatureVector(temp);
			
			
			// if we're doing feedback
			if(fbDocs > 0 && fbTerms > 0) {
				List<TResult> results = client.search(queryText, query.getQuerytweettime(), fbDocs);
				FeedbackRelevanceModel fb = new FeedbackRelevanceModel();
				fb.setOriginalQuery(query);
				fb.setRes(results);
				fb.build(stopper);
				
				FeatureVector fbVector = fb.asFeatureVector();
				fbVector.pruneToSize(fbTerms);
				fbVector.normalizeToOne();
				fbVector = FeatureVector.interpolate(query.getFeatureVector(), fbVector, ORIG_QUERY_WEIGHT);
		
				System.err.println(fbVector);
				
				StringBuilder builder = new StringBuilder();
				Iterator<String> terms = fbVector.iterator();
				while(terms.hasNext()) {
					String term = terms.next();
					if(term.length() < 2)
						continue;
					double prob = fbVector.getFeaturetWeight(term);
					builder.append(term + "^" + prob + " ");
				}
				queryText = builder.toString().trim();
				
			}
			
			List<TResult> results = client.search(queryText, query.getQuerytweettime(), numResults);
			String runTag = params.getParamValue(RUNTAG_OPTION);
			if(runTag==null) 
				runTag = DEFAULT_RUNTAG;

			int i = 1;
			Iterator<TResult> hitIterator = results.iterator();
			while(hitIterator.hasNext()) {
				TResult hit = hitIterator.next();
				out.println(String.format("%s Q0 %s %d %f %s", query.getTitle(), hit.getId(), i,
						hit.getRsv(), runTag));

				if(i++ >= numResults)
					break;
			}

		}
		out.close();
	}
}
