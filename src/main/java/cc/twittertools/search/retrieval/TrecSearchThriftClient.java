package cc.twittertools.search.retrieval;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import cc.twittertools.thrift.gen.TQuery;
import cc.twittertools.thrift.gen.TResult;
import cc.twittertools.thrift.gen.TrecSearch;

import com.google.common.base.Preconditions;

public class TrecSearchThriftClient {
  private final String group;
  private final String token;
  private final String host;
  private final int port;

  public TrecSearchThriftClient(String host, int port,
      @Nullable String group, @Nullable String token) {
    Preconditions.checkNotNull(host);
    Preconditions.checkArgument(port > 0);
    this.group = group;
    this.token = token;
    this.host= host;
    this.port = port;
  }

  public List<TResult> search(String query, long maxId, int numResults) throws TException {
    TTransport transport = new TSocket(host, port);
    transport.open();

    TrecSearch.Client client = new TrecSearch.Client(new TBinaryProtocol(transport));

    TQuery q = new TQuery();
    q.text = query;
    q.max_id = maxId;
    q.num_results = numResults;

    q.group = group;
    q.token = token;

    List<TResult> results = client.search(q);
    transport.close();

    return results;
  }
}
