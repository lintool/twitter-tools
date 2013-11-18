package cc.twittertools.thrift.gen;

public class TResultComparator implements Comparable<TResultComparator> {
  private TResult tresult;
  public TResultComparator(TResult tresult) {
	this.tresult = tresult;
  }
  public TResult getTResult() {
	return tresult;
  }
  public int compareTo(TResultComparator other) {
    if (tresult.id > other.tresult.id) return -1;
	else if (tresult.id < other.tresult.id) return 1;
	else return 0;
  }
  public boolean equals(Object other) {
	  if (other == null) return false;
	  if (other.getClass() != this.getClass()) return false;
	  return ((TResultComparator)other).tresult.id == this.tresult.id;
  }
}