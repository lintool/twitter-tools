package cc.twittertools.search.retrieval;


import cc.twittertools.search.retrieval.QueryEnvironment;


public class RunQuery {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
	  
	  try {
		String pathToIndexFile = args[0];
		String pathToQueryFile = args[1];
		
		QueryEnvironment env = new QueryEnvironment();
		env.setPathToIndexFile(pathToIndexFile);
		env.setPathToQueryFile(pathToQueryFile);
		
		
		env.parseParams();
		
		
		env.runQueries();
		
	  } catch (Exception e) {
	    RunQuery.help();
	    e.printStackTrace();
	    System.exit(-1);
	  }

	}

	 public static void help() {
	    System.err.println("expected arguments: /path/to/run_config/file /path/to/query/file");
	    System.err.println();
	    System.err.println("where run_config file is of the structure:");
	    System.err.println();
	    System.err.println("<parameters>");
	    System.err.println("<index>/path/to/index/to/search</index>");
	    System.err.println("<count>num_docs_per_query</count>");
	    System.err.println("<similarity>[default, bm25, lm]</similarity> [optional]");
	    System.err.println("</parameters>");
	    System.err.println();
	    System.err.println("and the query file contains indri-type enumeration of test queries.");



	  }
	 
}
