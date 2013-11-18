package cc.twittertools.search.api;

import cc.twittertools.thrift.gen.TResult;

public class TResultComparable implements Comparable<TResultComparable> {
  private TResult tresult;
  public TResultComparable(TResult tresult) {
    this.tresult = tresult;
  }
  public TResult getTResult() {
    return tresult;
  }
  public int compareTo(TResultComparable other) {
    if (tresult.rsv > other.tresult.rsv) return -1;
    else if (tresult.rsv < other.tresult.rsv) return 1;
    else {
      if (tresult.id > other.tresult.id) return -1;
      else if (tresult.id < other.tresult.id) return 1;
      else return 0;
    }
  }
  public boolean equals(Object other) {
    if (other == null) return false;
    if (other.getClass() != this.getClass()) return false;
    return ((TResultComparable)other).tresult.id == this.tresult.id;
  }
}