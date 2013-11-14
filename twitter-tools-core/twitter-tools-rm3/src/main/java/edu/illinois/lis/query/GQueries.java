package edu.illinois.lis.query;

import java.util.Iterator;

/**
 * A container for holding a bunch of GQuery objects, with various types of convenience functionality added in 
 * instantiating classes.
 * 
 * @author Miles Efron
 *
 */
public interface GQueries {
	public void read(String pathToQueries);
		
	public Iterator<GQuery> iterator();
	
	public GQuery getIthQuery(int i);
	
	public GQuery getNamedQuery(String queryName);
	
	public int numQueries();
}
