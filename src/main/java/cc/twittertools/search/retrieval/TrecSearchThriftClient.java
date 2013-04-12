package cc.twittertools.search.retrieval;

import java.util.List;

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

  public TrecSearchThriftClient(String host, int port) throws TTransportException {
    Preconditions.checkNotNull(host);
    Preconditions.checkArgument(port > 0);

    transport = new TSocket(host, port);
    transport.open();

    client = new TrecSearch.Client(new TBinaryProtocol(transport));
  }

  public List<TResult> search(TQuery q) throws TException {
    return client.search(q);
  }

  public void close() {
    transport.close();
  }
}
