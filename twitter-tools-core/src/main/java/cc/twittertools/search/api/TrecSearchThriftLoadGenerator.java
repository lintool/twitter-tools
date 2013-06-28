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

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import cc.twittertools.thrift.gen.TResult;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

public class TrecSearchThriftLoadGenerator {
  private static final Logger LOG = Logger.getLogger(TrecSearchThriftLoadGenerator.class);

  public static class WorkerThread implements Runnable {
    private final TrecSearchThriftClient client;
    private final ConcurrentLinkedQueue<String> queue;
    private final AtomicInteger errorCounter;
    private final AtomicInteger latencyCounter;

    public WorkerThread(TrecSearchThriftLoadGenerator generator, String host, int port,
        String group, String token) throws Exception {
      Preconditions.checkNotNull(host);
      Preconditions.checkArgument(port > 0);
      Preconditions.checkNotNull(generator);

      this.queue = generator.getQueue(); 
      this.errorCounter = generator.getErrorCounter();
      this.latencyCounter = generator.getLatencyCounter();
      this.client = new TrecSearchThriftClient(host, port, group, token);
    }

    @Override
    public void run() {
      LOG.info(Thread.currentThread().getName() + " starting...");

      long startTime = 0;
      String queryString = null;
      while ((queryString = queue.poll()) != null) {
        startTime = System.currentTimeMillis();

        try {
          @SuppressWarnings("unused")
          List<TResult> results = client.search(queryString, Long.MAX_VALUE, 1000);
          // Don't do anything with the result.
          int t = (int) (System.currentTimeMillis() - startTime);
          LOG.info(String.format("%s: %4dms for query \"%s\"",
              Thread.currentThread().getName(), t, queryString));
          latencyCounter.addAndGet(t);
        } catch (TException e) {
          errorCounter.incrementAndGet();
          LOG.info(String.format("%s: error recorded for query \"%s\"",
              Thread.currentThread().getName(), queryString));
        }
      }

      LOG.info(Thread.currentThread().getName() + " finished.");
    }
  }

  private static final int maxThreads = 32;

  private int threadCount = 4;
  private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
  private final ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
  private final AtomicInteger errorCounter = new AtomicInteger();
  private final AtomicInteger latencyCounter = new AtomicInteger();
  private final int queueSize;

  private String group = null;
  private String token = null;

  public TrecSearchThriftLoadGenerator(File queryFile, int limit) throws Exception {
    Preconditions.checkNotNull(queryFile);
    Preconditions.checkArgument(queryFile.exists());

    List<String> lines = Files.readLines(queryFile, Charsets.UTF_8);

    int cnt = 1;
    for (String q : lines) {
      queue.add(q);
      cnt++;
      if (cnt > limit) {
        break;
      }
    }

    queueSize = queue.size();
  }

  public TrecSearchThriftLoadGenerator withCredentials(String group, String token) {
    this.group = group;
    this.token = token;

    return this;
  }

  public TrecSearchThriftLoadGenerator withThreads(int n) {
    // Make sure number of threads is a reasonable setting.
    Preconditions.checkArgument(n > 0 && n < 32);
    this.threadCount = n;

    return this;
  }

  public ConcurrentLinkedQueue<String> getQueue() {
    return queue;
  }

  public AtomicInteger getErrorCounter() {
    return errorCounter;
  }

  public AtomicInteger getLatencyCounter() {
    return latencyCounter;
  }

  public void run(String host, int port) throws Exception {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < threadCount; i++) {
      Runnable worker = new WorkerThread(this, host, port, group, token);
      executor.execute(worker);
    }
    executor.shutdown();
    while (!executor.isTerminated()) {}

    long totalTime = (System.currentTimeMillis() - startTime);
    LOG.info("Finished all threads!");
    LOG.info("Total time: " + totalTime + " ms");
    LOG.info("Number of queries: " + queueSize);
    LOG.info(String.format("Throughput: %.2f qps", 1000.0/(totalTime/queueSize)));
    LOG.info(String.format("Latency: %d ms", latencyCounter.intValue()/queueSize));
    LOG.info("Errors: " + errorCounter.get());
  }

  private static final int DEFAULT_PORT = 9090;
  private static final int DEFAULT_THREADS = 4;

  private static final String HELP_OPTION = "h";
  private static final String HOST_OPTION = "host";
  private static final String PORT_OPTION = "port";
  private static final String THREADS_OPTION = "threads";
  private static final String LIMIT_OPTION = "limit";
  private static final String GROUP_OPTION = "group";
  private static final String TOKEN_OPTION = "token";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(HELP_OPTION, "show help"));
    options.addOption(OptionBuilder.withArgName("port").hasArg()
        .withDescription("port").create(PORT_OPTION));
    options.addOption(OptionBuilder.withArgName("index").hasArg()
        .withDescription("host").create(HOST_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("threads").create(THREADS_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of queries to process").create(LIMIT_OPTION));
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

    if (cmdline.hasOption(HELP_OPTION) || !cmdline.hasOption(HOST_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(TrecSearchThriftServer.class.getName(), options);
      System.exit(-1);
    }

    String host = cmdline.getOptionValue(HOST_OPTION);
    int port = cmdline.hasOption(PORT_OPTION) ?
        Integer.parseInt(cmdline.getOptionValue(PORT_OPTION)) : DEFAULT_PORT;
    int numThreads = cmdline.hasOption(THREADS_OPTION) ?
        Integer.parseInt(cmdline.getOptionValue(THREADS_OPTION)) : DEFAULT_THREADS;
    int limit = cmdline.hasOption(LIMIT_OPTION) ?
        Integer.parseInt(cmdline.getOptionValue(LIMIT_OPTION)) : Integer.MAX_VALUE;

    String group = cmdline.hasOption(GROUP_OPTION) ? cmdline.getOptionValue(GROUP_OPTION) : null;
    String token = cmdline.hasOption(TOKEN_OPTION) ? cmdline.getOptionValue(TOKEN_OPTION) : null;

    String queryFile = "data/queries.trec2005efficiency.txt";
    new TrecSearchThriftLoadGenerator(new File(queryFile), limit)
        .withThreads(numThreads)
        .withCredentials(group, token)
        .run(host, port);
  }
}
