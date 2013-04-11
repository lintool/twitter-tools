package cc.twittertools.search.retrieval;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

import cc.twittertools.thrift.gen.TrecSearch;

public class TrecSearchThriftServer {
  private static final int PORT = 9090;
  private static int numberThreads = 5;

  public static void main(String[] args) throws Exception {
    TServerSocket serverSocket = new TServerSocket(PORT, 100000);
    TrecSearch.Processor<TrecSearch.Iface> searchProcessor = new TrecSearch.Processor(new TrecSearchHandler("tweets2011-index"));
    if (args.length > 1) {
      numberThreads = Integer.parseInt(args[1]);
    }
    TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverSocket);
    serverArgs.maxWorkerThreads(numberThreads);
    TServer thriftServer = new TThreadPoolServer(serverArgs.processor(searchProcessor)
        .protocolFactory(new TBinaryProtocol.Factory()));
    thriftServer.serve();
  }
}