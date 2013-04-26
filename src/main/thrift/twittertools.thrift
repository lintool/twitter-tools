namespace java cc.twittertools.thrift.gen

struct TResult {
  1: i64 id,
  2: double rsv,
  3: string screen_name,
  4: string created_at,
  5: string text
}

struct TQuery {
  1: string group,
  2: string token,
  3: string text,
  4: i64 max_id,
  5: i32 num_results
}
 
exception TrecSearchException {
  1: string message
}
 
service TrecSearch {
  list<TResult> search(1: TQuery query)
    throws (1: TrecSearchException error)
}
