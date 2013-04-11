package cc.twittertools.search.retrieval;

import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import cc.twittertools.thrift.gen.TrecSearch;
import cc.twittertools.thrift.gen.TrecSearchQuery;
import cc.twittertools.thrift.gen.TrecSearchResult;

public class TrecSearchThriftClientCli {
  public static void main(String[] args) {

    try {
      TTransport transport;

      transport = new TSocket("localhost", 9090);
      transport.open();

      TProtocol protocol = new TBinaryProtocol(transport);
      TrecSearch.Client client = new TrecSearch.Client(protocol);

      TrecSearchQuery q = new TrecSearchQuery();
      q.text = "BBC World Service staff cuts";
      q.max_uid = 34952194402811905L;
      q.num_results = 1000;

      List<TrecSearchResult> results = client.search(q);
      int i = 0;
      for (TrecSearchResult result : results) {
        System.out.println("MB01 Q0 " + result.id + " " + i + " " + result.rsv + " lucy");
        i++;
      }      

      transport.close();
    } catch (TTransportException e) {
      e.printStackTrace();
    } catch (TException x) {
      x.printStackTrace();
    }
  }

}