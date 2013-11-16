package edu.illinois.lis.utils;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

public class Qrels {

	public static final Pattern SPACE_PATTERN = Pattern.compile(" ", Pattern.DOTALL);
	
	private static final int QUERY_COLUMN = 0;
	private static final int DOCNO_COLUMN = 2;
	private static final int REL_COLUMN   = 3;
	
	private Map<String,Set<String>> rel;
	private int minRel = 1;
	
	public Qrels(String pathToQrelsFile) {
		try {
			
			rel = new HashMap<String,Set<String>>();
			
			List<String> lines = IOUtils.readLines(new FileReader(new File(pathToQrelsFile)));
			Iterator<String> linesIt = lines.iterator();
			while(linesIt.hasNext()) {
				String[] toks = SPACE_PATTERN.split(linesIt.next());
				if(toks==null || toks.length != 4) {
					System.err.println("bad qrels line");
					continue;
				}
				String query = toks[QUERY_COLUMN];
				String docno = toks[DOCNO_COLUMN];
				int r = Integer.parseInt(toks[REL_COLUMN]);
				if(r >= minRel) {
					Set<String> relDocs = null;
					if(!rel.containsKey(query)) {
						relDocs = new HashSet<String>();
					} else {
						relDocs = rel.get(query);
					}
					relDocs.add(docno);
					rel.put(query, relDocs);
				} else {
				}
			}
		} catch (Exception e) {
			System.err.println("died trying to read qrel file: " + pathToQrelsFile);
			System.exit(-1);
		}
	}
	
	public boolean isRel(String query, String docno) {
		if(!rel.containsKey(query)) {
			System.err.println("no relevant documents found for query " + query);
			return false;
		}
		return rel.get(query).contains(docno);
	}
	
	public Set<String> getRelDocs(String query) {
		if(!rel.containsKey(query)) {
			System.err.println("no relevant documents found for query " + query);
			return null;
		}
		return rel.get(query);
	}
	
	public double numRel(String query) {
		if(!rel.containsKey(query)) {
			System.err.println("no relevant documents found for query " + query);
			return 0.0;
		}
		return (double)rel.get(query).size();
	}
}
