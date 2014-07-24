package edu.gslis.ttg.clusters;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import edu.gslis.eval.Qrels;
import edu.gslis.queries.GQuery;

public class Cluster {
	private Set<Long> members;
	
	public Cluster() {
		members = new HashSet<Long>();
	}
	
	public Cluster(long member) {
		members = new HashSet<Long>();
		members.add(member);
	}
	
	public void add(long member) {
		members.add(member);
	}
	
	public void add(Set<Long> newMembers) {
		members.addAll(newMembers);
	}
	
	public Set<Long> getMembers() {
		return members;
	}
	
	public long getFirstMember() {
		return members.iterator().next();
	}
	
	public boolean hasMember(long member) {
		return members.contains(member);
	}
	
	public int getWeight(GQuery query, Qrels qrels) {
		// hack to change e.g. MB01 to 01
		String q = String.valueOf(Integer.parseInt(query.getTitle().substring(2, query.getTitle().length())));
		
		int weight = 0;
		for (long member : members) {
			if (qrels.isRel(q, String.valueOf(member))) {
				int level = qrels.getRelLevel(q, String.valueOf(member));
				weight += level;
			}
		}
		return weight;
	}
	
	@Override
	public String toString() {
		return Arrays.deepToString(members.toArray());
	}
	
	public int size() {
		return members.size();
	}

}
