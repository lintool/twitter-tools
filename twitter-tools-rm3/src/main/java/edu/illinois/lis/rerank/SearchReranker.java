package edu.illinois.lis.rerank;

import java.util.Collections;
import java.util.List;


import cc.twittertools.thrift.gen.TResult;


public abstract class SearchReranker {
	protected List<TResult> results;
	
	protected abstract void score();
	
	public List<TResult> getReranked() {
		TResultComparator comparator = new TResultComparator(true);
		Collections.sort(results, comparator);
		return results;
	}
}
