package com.twitter.corpus.download;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHandler.STATE;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;
import com.ning.http.client.Response;
import com.ning.http.client.extra.ThrottleRequestFilter;
import com.twitter.corpus.demo.ReadStatuses;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

/**
 * Includes some code by spacelis@github
 */
public class AsyncEmbeddedJsonStatusBlockCrawler {
    private static final Logger LOG = Logger.getLogger(AsyncEmbeddedJsonStatusBlockCrawler.class);
    private static final int TWEET_BLOCK_SIZE = 500;

    private static final String JSON_START = "<input type=\"hidden\" id=\"init-data\" class=\"json-data\" value=\"";
    private static final String JSON_END = "\">";

    private static final int MAX_CONNECTIONS = 100;
    private static final int CONNECTION_TIMEOUT = 10000;
    private static final int IDLE_CONNECTION_TIMEOUT = 10000;
    private static final int REQUEST_TIMEOUT = 10000;
    private static final int MAX_RETRY_ATTEMPTS = 2;
    private static final int WAIT_BEFORE_RETRY = 1000;
    private static final Timer timer = new Timer(true);

    private final File file;
    private final String output;
    private final AsyncHttpClient asyncHttpClient;
    private final boolean noFollow;

    // key = statud id, value = tweet JSON
    private final ConcurrentSkipListMap<Long, String> crawl =
        new ConcurrentSkipListMap<Long, String>();

    private final AtomicInteger connections = new AtomicInteger(0);

    private static final JsonParser parser = new JsonParser();
    private static final Gson gson = new Gson();

    public AsyncEmbeddedJsonStatusBlockCrawler(File file, String output, boolean noFollow) throws IOException {
        this.file = Preconditions.checkNotNull(file);
        this.output = Preconditions.checkNotNull(output);
        this.noFollow = noFollow;

        if (!file.exists()) {
            throw new IOException(file + " does not exist!");
        }

        AsyncHttpClientConfig config = new AsyncHttpClientConfig.Builder()
            .addRequestFilter(new ThrottleRequestFilter(MAX_CONNECTIONS))
            .setConnectionTimeoutInMs(CONNECTION_TIMEOUT)
            .setIdleConnectionInPoolTimeoutInMs(IDLE_CONNECTION_TIMEOUT)
            .setRequestTimeoutInMs(REQUEST_TIMEOUT)
            .setMaxRequestRetry(0)
            .build();
        this.asyncHttpClient = new AsyncHttpClient(config);
    }

    public static String getUrl(long id, String username) {
        Preconditions.checkNotNull(username);
        return String.format("http://twitter.com/%s/status/%d", username, id);
    }

    public void fetch() throws IOException {
        long start = System.currentTimeMillis();
        LOG.info("Processing " + file);

        int cnt = 0;
        try {
            BufferedReader data = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String line;
            while ((line = data.readLine()) != null) {
                try {
                    String[] arr = line.split("\t");
                    long id = Long.parseLong(arr[0]);
                    String username = (arr.length > 1) ? arr[1] : "a";
                    String url = getUrl(id, username);

                    connections.incrementAndGet();
                    crawlURL(url, new TweetFetcherHandler(id, username, url, 0, !this.noFollow));

                    cnt++;

                    if (cnt % TWEET_BLOCK_SIZE == 0) {
                        LOG.info(cnt + " requests submitted");
                    }
                } catch (NumberFormatException e) { // parseLong
                    continue;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Wait for the last requests to complete.
        LOG.info("Waiting for remaining requests (" + connections.get() + ") to finish!");
        for (int i = 0; i < 10; i++) {
            if (connections.get() == 0) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        asyncHttpClient.close();

        long end = System.currentTimeMillis();
        long duration = end - start;
        LOG.info("Total request submitted: " + cnt);
        LOG.info(crawl.size() + " tweets fetched in " + duration + "ms");

        LOG.info("Writing tweets...");
        int written = 0;
        Configuration conf = new Configuration();

        OutputStreamWriter out =
            new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(output)));
        for (Map.Entry<Long, String> entry : crawl.entrySet()) {
            written++;
            out.write(entry.getValue() + "\n");
        }
        
        out.close();

        LOG.info(written + " statuses written.");
        LOG.info("Done!");
    }

    private class TweetFetcherHandler extends AsyncCompletionHandler<Response> {
        private final long id;
        private final String username;
        private final String url;
        private final int numRetries;
        private final boolean followRedirects;

        private int httpStatus = -1;

        public TweetFetcherHandler(long id, String username, String url, int numRetries, boolean followRedirects) {
            this.id = id;
            this.username = username;
            this.url = url;
            this.numRetries = numRetries;
            this.followRedirects = followRedirects;
        }

        @Override
            public STATE onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
                this.httpStatus = responseStatus.getStatusCode();
                switch (this.httpStatus) {
                    case 404:
                        LOG.warn("Abandoning missing page: " + url);
                        connections.decrementAndGet();
                        return STATE.ABORT;

                    case 500:
                        //LOG.warn("Error status " + this.httpStatus + ": " + url);
                        retry();
                        return STATE.ABORT;
                }

                return super.onStatusReceived(responseStatus);
            }

        @Override
            public STATE onHeadersReceived(HttpResponseHeaders headers) throws Exception {
                switch (this.httpStatus) {
                    case 301:
                    case 302:
                        String redirect = headers.getHeaders().getFirstValue("Location");
                        if (redirect.contains("protected_redirect=true")) {
                            LOG.warn("Abandoning protected account: " + url);
                            connections.decrementAndGet();
                        } else if (redirect.contains("account/suspended")) {
                            LOG.warn("Abandoning suspended account: " + url);
                            connections.decrementAndGet();
                        } else if (followRedirects) {
                            //LOG.warn("Following redirect: " + url);
                            crawlURL(redirect, new TweetFetcherHandler(id, username, redirect, numRetries, followRedirects));
                        } else {
                            LOG.warn("Abandoning redirect: " + url);
                            connections.decrementAndGet();
                        }
                        return STATE.ABORT;
                }

                return super.onHeadersReceived(headers);
            }

        @Override
            public Response onCompleted(Response response) {
                switch (this.httpStatus) {
                    case -1:
                    case 301:
                    case 302:
                    case 404:
                    case 500:
                        return response;
                }

                // extract embedded JSON
                try {
                    String html = response.getResponseBody("UTF-8");
                    int jsonStart = html.indexOf(JSON_START);
                    int jsonEnd = html.indexOf(JSON_END, jsonStart + JSON_START.length());
                    if (jsonStart < 0 || jsonEnd < 0) {
                        LOG.warn("Unable to find embedded JSON: " + url);
                        retry();
                        return response;
                    }
                    String json = html.substring(jsonStart + JSON_START.length(), jsonEnd);
                    json = StringEscapeUtils.unescapeHtml(json);
                    JsonObject page = (JsonObject)parser.parse(json);
                    String status = gson.toJson(page
                            .getAsJsonObject("embedData")
                            .getAsJsonObject("status"));
                    crawl.put(id, status);
                } catch (IOException e) {
                    LOG.warn("Error (" + e + "): " + url);
                    retry();
                    return response;
                } catch (JsonSyntaxException e) {
                    LOG.warn("Unable to parse embedded JSON: " + url);
                    retry();
                    return response;
                } catch (NullPointerException e) {
                    LOG.warn("Unexpected format for embedded JSON: " + url);
                    retry();
                    return response;
                }

                connections.decrementAndGet();

                return response;
            }

        @Override
            public void onThrowable(Throwable t) {
                //LOG.warn("Error (" + t + "): " + url);
                retry();
            }

        private void retry() {
            if (this.numRetries >= MAX_RETRY_ATTEMPTS) {
                LOG.warn("Abandoning after max retry attempts: " + url);
                connections.decrementAndGet();
                return;
            }
            //LOG.warn("Retrying (attempt " + (numRetries+1) + "): " + url);
            timer.schedule(new RetryTask(id, username, url, numRetries+1, followRedirects),
                    WAIT_BEFORE_RETRY);
        }

        private class RetryTask extends TimerTask {
            private final long id;
            private final String username;
            private final String url;
            private final int numRetries;
            private final boolean followRedirects;

            public RetryTask(long id, String username, String url, int numRetries, boolean followRedirects) {
                this.id = id;
                this.username = username;
                this.url = url;
                this.numRetries = numRetries;
                this.followRedirects = followRedirects;
            }

            public void run() {
                crawlURL(url, new TweetFetcherHandler(id, username, url, numRetries, followRedirects));
            }
        }
    }

    private void crawlURL(String url, TweetFetcherHandler handler) {
        try {
            asyncHttpClient.prepareGet(url)
                .addHeader("Accept-Charset", "utf-8")
                .addHeader("Accept-Language", "en-US")
                .execute(handler);
        } catch (IOException e) {
            LOG.warn("Abandoning due to error (" + e + "): " + url);
            connections.decrementAndGet();
        }
    }

    private static final String DATA_OPTION = "data";
    private static final String OUTPUT_OPTION = "output";
    private static final String NOFOLLOW_OPTION = "noFollow";

    @SuppressWarnings("static-access")
        public static void main(String[] args) throws Exception {
            Options options = new Options();
            options.addOption(OptionBuilder.withArgName("path").hasArg()
                    .withDescription("data file with tweet ids").create(DATA_OPTION));
            options.addOption(OptionBuilder.withArgName("path").hasArg()
                    .withDescription("output file (*.gz)").create(OUTPUT_OPTION));
            options.addOption(NOFOLLOW_OPTION, NOFOLLOW_OPTION, false, "don't follow 301 redirects");

            CommandLine cmdline = null;
            CommandLineParser parser = new GnuParser();
            try {
                cmdline = parser.parse(options, args);
            } catch (ParseException exp) {
                System.err.println("Error parsing command line: " + exp.getMessage());
                System.exit(-1);
            }

            if (!cmdline.hasOption(DATA_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(ReadStatuses.class.getName(), options);
                System.exit(-1);
            }

            String data = cmdline.getOptionValue(DATA_OPTION);
            String output = cmdline.getOptionValue(OUTPUT_OPTION);
            boolean noFollow = cmdline.hasOption(NOFOLLOW_OPTION);
            new AsyncEmbeddedJsonStatusBlockCrawler(new File(data), output, noFollow).fetch();
        }
}
