namespace java cc.twittertools.thrift.gen

struct TrecSearchResult {
  1: i64 id,
  2: double rsv,
  3: string screen_name,
  4: string created_at,
  5: string text
}

struct TrecSearchQuery {
  1: string text,
  2: i64 max_uid,
  3: i32 num_results
}
 
exception TrecSearchException {
  1: string message
}
 
service TrecSearch {
  list<TrecSearchResult> search(1: TrecSearchQuery query)
    throws (1: TrecSearchException error)
}
