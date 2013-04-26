package cc.twittertools.search.retrieval;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import cc.twittertools.thrift.gen.TQuery;
import cc.twittertools.thrift.gen.TResult;
import cc.twittertools.thrift.gen.TrecSearch;

import com.google.common.base.Preconditions;

public class TrecSearchThriftClient {
  private final TTransport transport;
  private final TrecSearch.Client client;
  private final String group;
  private final String token;

  public TrecSearchThriftClient(String host, int port,
      @Nullable String group, @Nullable String token) throws TTransportException {
    Preconditions.checkNotNull(host);
    Preconditions.checkArgument(port > 0);
    this.group = group;
    this.token = token;

    transport = new TSocket(host, port);
    transport.open();

    client = new TrecSearch.Client(new TBinaryProtocol(transport));
  }

  public List<TResult> search(String query, long maxId, int numResults) throws TException {
    TQuery q = new TQuery();
    q.text = query;
    q.max_id = maxId;
    q.num_results = numResults;

    q.group = group;
    q.token = token;

    return client.search(q);
  }

  public void close() {
    transport.close();
  }
}
