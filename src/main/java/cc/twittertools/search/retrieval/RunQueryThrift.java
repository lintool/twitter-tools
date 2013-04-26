package cc.twittertools.search.retrieval;

import java.io.File;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cc.twittertools.search.configuration.IndriQueryParams;
import cc.twittertools.thrift.gen.TResult;

public class RunQueryThrift {
  private static final String HELP_OPTION = "h";
  private static final String HOST_OPTION = "host";
  private static final String PORT_OPTION = "port";
  private static final String QUERIES_OPTION = "queries";
  private static final String GROUP_OPTION = "group";
  private static final String TOKEN_OPTION = "token";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(HELP_OPTION, "show help"));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("host").create(HOST_OPTION));
    options.addOption(OptionBuilder.withArgName("port").hasArg()
        .withDescription("port").create(PORT_OPTION));
    options.addOption(OptionBuilder.withArgName("file").hasArg()
        .withDescription("XML file containing queries").create(QUERIES_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("group id").create(GROUP_OPTION));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("access token").create(TOKEN_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(HOST_OPTION) || !cmdline.hasOption(PORT_OPTION)
        || !cmdline.hasOption(QUERIES_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(TrecSearchThriftClientCli.class.getName(), options);
      System.exit(-1);
    }

    String queryFile = cmdline.getOptionValue(QUERIES_OPTION);
    if (!new File(queryFile).exists()) {
      System.err.println("Error: " + queryFile + " doesn't exist!");
      System.exit(-1);
    }

    IndriQueryParams queryParams = new IndriQueryParams();
    queryParams.ParseXMLQueryFile(queryFile);

    String group = cmdline.hasOption(GROUP_OPTION) ? cmdline.getOptionValue(GROUP_OPTION) : null;
    String token = cmdline.hasOption(TOKEN_OPTION) ? cmdline.getOptionValue(TOKEN_OPTION) : null;

    TrecSearchThriftClient client = new TrecSearchThriftClient(cmdline.getOptionValue(HOST_OPTION),
        Integer.parseInt(cmdline.getOptionValue(PORT_OPTION)), group, token);

    Queries queries = queryParams.getQueries();
    Query query;
    while((query = queries.getNextQuery()) != null) {
      List<TResult> results = client.search(query.getQueryString(),
          Long.parseLong(query.getMetadataField("lastrel")), 10000);
      int i = 1;
      for (TResult result : results) {
        System.out.println(query.getQueryName() + " Q0 " + result.id + " " + i + " " + result.rsv + " lucy");
        i++;
      }
      
    }

    client.close();
  }
}
