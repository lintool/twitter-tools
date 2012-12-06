twitter-corpus-tools
====================

These tools associated with the tweets corpus prepared for the [TREC 2011 Microblog Task](https://sites.google.com/site/microblogtrack/). The mailing list is [ trec-microblog@googlegroups.com](http://groups.google.com/group/trec-microblog).

The corpus is distributed as directories, each of which contains approximately 100 `.dat` files, each of which contains a list of (tweet id, username, MD5 checksum). Each of these files is referred to as a status block (i.e., block of tweets).

Once you've cloned the repository, you should be able to type `ant` to build the tools.

There are two ways to obtain the actual corpus:

- **Using the Twitter API.** Typically, a client is limited to 150 API calls an hour, so downloading the corpus this way will take a long time! However, some people have a whitelisted API key (which unfortunately is no longer available), which supports 20k API calls an hour. For those with this level of access, this method is preferred. The output of such a crawl is a JSON status block: each status is encoded as a JSON object, one status per line. The entire file is gzipped. 
- **Crawling twitter.com.** The combination of tweet id and username straightforwardly maps to a URL, which can be retrieved. Think `curl`. On steroids. On the up side: there's no rate limit. On the down side: we're essentially screen-scraping to reconstruct the original tweet, so the available data is not as rich as in the JSON structures. The output of such a crawl is a block-compressed Hadoop SequenceFile, where each key is a (tweet id, username) pair and each value is a complex object holding the raw HTML and other metadata. Note that we're storing the raw HTML and screen-scraping "on the fly", so that changes to the screen-scraper do not require recrawling the URLs. (Side note: `thrift` or `protobuf` fans will surely point out the many issues associated with Hadoop SequenceFiles. Yes, I know. Contributions welcome.)

Fetching a status block by crawling twitter.com (HTML output)
-------------------------------------------------------------

The HTML crawler is `com.twitter.corpus.download.AsyncEmbeddedJsonStatusBlockCrawler`. Here's a sample invocation:

    java -Xmx4g -cp 'lib/*:dist/twitter-corpus-tools-0.0.1.jar' com.twitter.corpus.download.AsyncEmbeddedJsonStatusBlockCrawler \
       -data 20110123/20110123-000.dat -output json/20110123-000.json.gz

Use the `-data` option to specify the status block (`.dat` file) to read. Use the `-output` option to specify where to write the output (gzip-compressed JSON file).

**Note:** On Windows machines, the classpath specified by `-cp` should be separated by semicolons.

To download the entire corpus, you'll need to fetch all blocks using this crawler. It makes sense to do some lightweight scripting (e.g., shell scripts, Python, Perl, etc.) to accomplish this. We have decided not to include these scripts because 1.) they are easy enough to write, and 2.) you may wish to download the blocks in parallel from multiple machines, in which no general purpose script would be particularly useful.

**Note:** Please be considerate when downloading the corpus. Using a couple of machines is fine. Writing a Hadoop MapReduce job to download the corpus from 500 EC2 instances _is not_. Use common sense.

Despite best efforts handling timeouts and retrying, the crawler may not successfully download all statuses in one go. If you absolutely need all available tweets, you should rerun the crawler one or more times with just the missing tweet ids. I've found that this process has diminishing returns after one or two iterations.


Fetching a status block via the Twitter API (JSON output)
--------------------------------------------------------

The REST API crawler is `com.twitter.corpus.download.AsyncJsonStatusBlockCrawler`. Here's a sample invocation:

    java -Xmx4g -cp 'lib/*:dist/twitter-corpus-tools-0.0.1.jar' com.twitter.corpus.download.AsyncJsonStatusBlockCrawler \
       -data 20110123/20110123-000.dat -output json/20110123-000.json.gz

Use the `-data` option to specify the status block (`.dat` file) to read. Use the `-output` option to specify where to write the output (gzipped JSON-encoded statuses, one per line).

To download the entire corpus, you'll need to fetch all blocks using this crawler.

Despite best efforts handling timeouts and retrying, the crawler may not successfully download all statuses in one go. To address this issue, there is a "repair" program that will fetch statuses that went missing the first time around. Here's a sample invocation:

    java -Xmx4g -cp 'lib/*:dist/twitter-corpus-tools-0.0.1.jar' com.twitter.corpus.download.VerifyJsonStatusBlockCrawl \
       -data 20110123/20110123-000.dat -statuses_input json/20110123-000.json.gz \
       -statuses_repaired json/20110123-000.repaired.json.gz \
       -output_success log.success -output_failure log.failure

Use the `-data` option to specify the status block (`.dat` file) to read. Use the `-statuses_input` to specify the output of the crawler; `-statuses_repaired` specifies where to put the repaired crawl. Options `-output_success` and `-output_failure` specify log files to hold tweet ids that were successfully and unsuccessfully fetched the first time around, respectively.


Reading the Corpus
------------------

The demo program `com.twitter.corpus.demo.ReadStatuses` provides a simple example of how to process the downloaded statuses, once you download the status blocks in either HTML or JSON. Here's a sample command-line invocation:

     java -cp lib/*:dist/twitter-corpus-tools-0.0.1.jar com.twitter.corpus.demo.ReadStatuses \
        -input html/20110123-000.html.seq -dump -html

Replace `-html` with `-json` if you want to read the JSON status blocks:

     java -cp lib/*:dist/twitter-corpus-tools-0.0.1.jar com.twitter.corpus.demo.ReadStatuses \
        -input json/20110123-000.json.gz -dump -json

Here's how you read all status blocks in a particular directory:

    java -cp lib/*:dist/twitter-corpus-tools-0.0.1.jar com.twitter.corpus.demo.ReadStatuses \
        -input html/ -html -verbose

Similarly, replace `-html` with `-json` if you want to read the JSON status blocks.


Searching the Corpus
--------------------

We've also provided a reference implementation of using Lucene to index the statuses:

    java -cp lib/*:dist/twitter-corpus-tools-0.0.1.jar com.twitter.corpus.demo.IndexStatuses \
        -input html/ -index index/ -html

Simple program to search the built index:

    java -cp lib/*:dist/twitter-corpus-tools-0.0.1.jar com.twitter.corpus.demo.SearchStatuses \
    -index index/ -query egypt -num_hits 100
