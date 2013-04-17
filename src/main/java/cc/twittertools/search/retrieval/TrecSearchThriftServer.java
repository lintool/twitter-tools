package cc.twittertools.search.retrieval;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

import cc.twittertools.thrift.gen.TrecSearch;

public class TrecSearchThriftServer {
  private static final int DEFAULT_PORT = 9090;
  private static final int DEFAULT_MAX_THREADS = 8;

  private static final String HELP_OPTION = "h";
  private static final String INDEX_OPTION = "index";
  private static final String PORT_OPTION = "port";
  private static final String MAX_THREADS_OPTION = "max_threads";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(HELP_OPTION, "show help"));
    options.addOption(OptionBuilder.withArgName("port").hasArg()
        .withDescription("port").create(PORT_OPTION));
    options.addOption(OptionBuilder.withArgName("index").hasArg()
        .withDescription("index location").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("max number of threads in thread pool").create(MAX_THREADS_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (cmdline.hasOption(HELP_OPTION) || !cmdline.hasOption(INDEX_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(TrecSearchThriftServer.class.getName(), options);
      System.exit(-1);
    }

    int port = cmdline.hasOption(PORT_OPTION) ?
        Integer.parseInt(cmdline.getOptionValue(PORT_OPTION)) : DEFAULT_PORT;
    int maxThreads = cmdline.hasOption(MAX_THREADS_OPTION) ?
        Integer.parseInt(cmdline.getOptionValue(MAX_THREADS_OPTION)) : DEFAULT_MAX_THREADS;
    File index = new File(cmdline.getOptionValue(INDEX_OPTION));

    if (!index.exists()) {
      System.err.println("Error: " + index + " does not exist!");
      System.exit(-1);
    }

    TServerSocket serverSocket = new TServerSocket(port);
    TrecSearch.Processor<TrecSearch.Iface> searchProcessor =
        new TrecSearch.Processor<TrecSearch.Iface>(new TrecSearchHandler(index));
    
    TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverSocket);
    serverArgs.maxWorkerThreads(maxThreads);
    TServer thriftServer = new TThreadPoolServer(serverArgs.processor(searchProcessor)
        .protocolFactory(new TBinaryProtocol.Factory()));

    thriftServer.serve();
  }
}