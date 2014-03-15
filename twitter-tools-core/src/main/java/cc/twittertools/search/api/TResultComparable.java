/**
 * Twitter Tools
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    if (tresult.rsv > other.tresult.rsv) {
      return -1;
    } else if (tresult.rsv < other.tresult.rsv) {
      return 1;
    } else {
      if (tresult.id > other.tresult.id) {
        return -1;
      } else if (tresult.id < other.tresult.id) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  public boolean equals(Object other) {
    if (other == null) {
      return false;
    } if (other.getClass() != this.getClass()) {
      return false;
    }

    return ((TResultComparable) other).tresult.id == this.tresult.id;
  }
}