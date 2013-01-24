Twitter Tools
=============

** version 1.0.0 **

These tools associated with the tweets corpus prepared for the [TREC Microblog Track](https://sites.google.com/site/microblogtrack/). The mailing list is [ trec-microblog@googlegroups.com](http://groups.google.com/group/trec-microblog).

The corpus is distributed as directories, each of which contains approximately 100 `.dat` files, each of which contains a list of (tweet id, username, MD5 checksum). Each of these files is referred to as a status block (i.e., block of tweets).

Once you've cloned the repository, you should be able to type `ant` to build the tools (assuming you have Ant installed). Ant will automatically create a script `etc/run.sh` that will be useful later.

The corpus downloader works by twitter.com. The combination of tweet id and username straightforwardly maps to a URL, which can be retrieved. Think `curl`. On steroids.

Sampling the public stream
--------------------------

Unlike in previous years, participants in the TREC 2013 microblog track will *not* be able to crawl a copy of the offical corpus. Participants will instead access the corpus through a REST API (details available elsewhere).

Should you wish to gather a parallel sample of tweets from the [Twitter public stream](https://dev.twitter.com/docs/streaming-apis/streams/public) from the same time period as the official corpus, you may do so using the `cc.twittertools.stream.GatherStatusStream` tool.

**IMPORTANT** Crawling your own copy of the tweets is NOT NEEDED for participation in the TREC 2013 microblog track. However, it might be helpful.

Accessing the Twitter public stream with the `GatherStatusStream` tool requires creating a developer account on Twitter and obtaining OAuth credentials to access Twitter's API. After creating an account on the Twitter developer site, you can obtain these credentials by [creating an "application"](https://dev.twitter.com/apps/new).

In order to run `GatherStatusStream`, you must save your Twitter API OAuth credentials in a file named `twitter4j.properties` in your current working directory. The file should contain the following (replace \*'s with your information):

    oauth.consumerKey=**********
    oauth.consumerSecret=**********
    oauth.accessToken=**********
    oauth.accessTokenSecret=**********

Once you have created the `twitter4j.properties` file, you can begin sampling from the public stream using the following invocation:

    etc/run.sh cc.twittertools.stream.GatherStatusStream

The tool will download JSON statuses continuously until it is stopped. Statuses will be saved in the current working directory and compressed hourly.

**Notes**

* For the official corpus to be used in the TREC 2013 microblog evaluation, crawling the public stream sample will commence on 2013/02/01 00:00:00 UTC and continue for the entire month of February and March, ending 2013/03/31 23:59:59 UTC.

* Not all JSON messages returned by the API correspond to tweets. In particular, some messages correspond to deleted tweets. See the [Twitter Streaming API page](https://dev.twitter.com/docs/streaming-apis/messages#Status_deletion_notices_delete) for details.

* The compressed file size of each hourly status block varies. In a 3-day sample from Jan 21-23, 2013, the average hourly block size was ~75MB.

Fetching a status block
-----------------------

The HTML crawler is `cc.twittertools.download.AsyncEmbeddedJsonStatusBlockCrawler`. Here's a sample invocation:

    etc/run.sh cc.twittertools.download.AsyncEmbeddedJsonStatusBlockCrawler \
       -data 20110123/20110123-000.dat -output json/20110123-000.json.gz

Use the `-data` option to specify the status block (`.dat` file) to read. Use the `-output` option to specify where to write the output (gzip-compressed JSON file).

To download the entire corpus, you'll need to fetch all blocks using this crawler. It makes sense to do some lightweight scripting (e.g., shell scripts, Python, Perl, etc.) to accomplish this. We have decided not to include these scripts because 1.) they are easy enough to write, and 2.) you may wish to download the blocks in parallel from multiple machines, in which no general purpose script would be particularly useful.

Despite best efforts handling timeouts and retrying, the crawler may not successfully download all statuses in one go. To address this issue, there is a "repair" command-line option that will output a new data file containing only those statuses that went missing the first time around. Here's a sample invocation:

    etc/run.sh cc.twittertools.download.AsyncEmbeddedJsonStatusBlockCrawler \
       -data 20110123/20110123-000.dat -output json/20110123-000.json.gz \
       -repair repair/20110123-000.dat

And the corresponding repair:

    etc/run.sh cc.twittertools.download.AsyncEmbeddedJsonStatusBlockCrawler \
       -data repair/20110123-000.dat -output json/20110123-000.repair.json.gz

**Notes:** 

* Please be considerate when downloading the corpus. Using a couple of machines is fine. Writing a Hadoop MapReduce job to download the corpus from 500 EC2 instances _is not_. Use common sense.

* A status will not be marked for repair if the account is protected, if the account has been suspended, or if the tweet has been deleted (404).

* There are known issues with retweets, which cause only the original tweet to appear in the output. In order to preserve the requested tweet id, the crawler therefore injects a 'requested\_id' field into each JSON status with the value of the originally requested (input) tweet id. Statuses where 'requested\_id' differs from 'id' can be considered to be retweets.

Reading the Corpus
------------------

The demo program `cc.twittertools.corpus.demo.ReadStatuses` provides a simple example of how to process the downloaded statuses, once you download the status blocks. Here's a sample command-line invocation:

     etc/run.sh cc.twittertools.corpus.demo.ReadStatuses \
        -input json/20110123-000.json.gz -dump

The `-input` option will also accept a directory, in which case it'll process all status blocks in the directory.


Searching the Corpus
--------------------

We've also provided a reference implementation of using Lucene to index the statuses:

    etc/run.sh cc.twittertools.corpus.demo.IndexStatuses \
        -input json/ -index index/

Simple program to search the built index:

    etc/run.sh cc.twittertools.corpus.demo.SearchStatuses \
        -index index/ -query egypt -num_hits 100
