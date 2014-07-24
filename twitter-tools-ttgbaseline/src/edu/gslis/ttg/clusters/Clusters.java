package edu.gslis.ttg.clusters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Clusters implements Iterable<Cluster> {
	private Set<Cluster> clusters;
	private Map<Long, Cluster> clusterMemberLookup;
	
	public Clusters() {
		clusters = new HashSet<Cluster>();
		clusterMemberLookup = new HashMap<Long, Cluster>();
	}
	
	public void add(Cluster cluster) {
		clusters.add(cluster);
		for (long member : cluster.getMembers()) {
			clusterMemberLookup.put(member, cluster);
		}
	}
	
	public Set<Cluster> getClusters() {
		return clusters;
	}
	
	public boolean hasCluster(Cluster cluster) {
		return clusters.contains(cluster);
	}
	
	public Cluster findCluster(long member) {
		try {
			return clusterMemberLookup.get(member);
		} catch (NullPointerException e) {
			return null;
		}
	}
	
	public Set<Long> getAllClusteredResults() {
		return clusterMemberLookup.keySet();
	}
	
	// Merge cluster 2 into cluster 1 and update the clusterMemberLookup
	// Note: only call this function if cluster 1 is already in the clusters set
	// 	(cluster 2 can be new or existing)
	public void mergeExistingClusters(Cluster c1, Cluster c2) {
		c1.add(c2.getMembers());
		clusters.remove(c1);
		try {
			clusters.remove(c2);
		} catch (Exception e) {
			System.err.println("Unable to remove cluster 2 from clusters. Might be a new cluster.");
		}
		clusters.add(c1);
		
		updateClusterMembership(c1);
	}
	
	// Merge two new clusters into the clusters set
	public void mergeNewClusters(Cluster c1, Cluster c2) {
		c1.add(c2.getMembers());
		clusters.add(c1);
		
		updateClusterMembership(c1);
	}
	
	public void mergeMembers(long m1, long m2) {
		Cluster c1 = findCluster(m1);
		Cluster c2 = findCluster(m2);
		if (c1 == null && c2 == null) {
			c1 = new Cluster(m1);
			c2 = new Cluster(m2);
			mergeNewClusters(c1, c2);
		} else if (c1 == null) { // c2 exists
			c1 = new Cluster(m1);
			mergeExistingClusters(c2, c1);
		} else { // c1 exists
			if (c2 == null) {
				c2 = new Cluster(m2);
			}
			mergeExistingClusters(c1, c2);
		}
	}
	
	public int size() {
		return clusters.size();
	}

	@Override
	public Iterator<Cluster> iterator() {
		return clusters.iterator();
	}
	
	@Override
	public String toString() {
		String output = "";
		output += "[";
		Iterator<Cluster> it = clusters.iterator();
		while (it.hasNext()) {
			Cluster cluster = it.next();
			output += cluster.toString();
			if (it.hasNext()) {
				output += ", ";
			}
		}
		output += "]";
		return output;
	}
	
	private void updateClusterMembership(Cluster cluster) {
		for (long member : cluster.getMembers()) {
			clusterMemberLookup.put(member, cluster);
		}
	}

}
