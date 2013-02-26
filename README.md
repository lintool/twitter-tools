Twitter Tools
=============

**version 1.1.1**

These tools associated with the tweets corpus prepared for the [TREC Microblog Track](https://sites.google.com/site/microblogtrack/). The mailing list is [ trec-microblog@googlegroups.com](http://groups.google.com/group/trec-microblog).

The Tweets2011 corpus, used in the TREC 2011 and 2012 microblog tracks, is distributed as directories, each of which contains approximately 100 `.dat` files, each of which contains a list of (tweet id, username, MD5 checksum). Each of these files is referred to as a status block (i.e., block of tweets).  The status block downloader works by fetching the tweets indicated in the `.dat` files from twitter.com. The combination of tweet id and username straightforwardly maps to a URL, which can be retrieved. Think `curl`. On steroids.

If you want to use this tool to download Tweets2011, or another static tweet collection distributed similarly, see the section below titled 'Fetching a status block'.

For TREC 2013, the microblog track corpus is a new Twitter public stream sample.  These tools will also enable you to do this sampling yourself, in real time, if you want to.  You can also use this tool to just sample the Twitter public stream for your own research, but remember, Twitter does not allow you to share the actual tweets.

Once you've cloned the repository, you should be able to type `ant` to build the tools (assuming you have Ant installed). Ant will automatically create a script `etc/run.sh` that will be useful later.

Sampling the public stream
--------------------------

Unlike in previous years, participants in the TREC 2013 microblog track will *not* be able to crawl a copy of the official corpus. Participants will instead access the corpus through a REST API (details will be made available elsewhere).

Should you wish to gather a parallel sample of tweets from the [Twitter public stream](https://dev.twitter.com/docs/streaming-apis/streams/public) from the same time period as the official corpus, you may do so using the `cc.twittertools.stream.GatherStatusStream` tool.

**IMPORTANT**: Crawling your own copy of the tweets is **not** **needed** for participation in the TREC 2013 microblog track. However, it might be helpful.

Accessing the Twitter public stream with the `GatherStatusStream` tool requires creating a developer account on Twitter and obtaining OAuth credentials to access Twitter's API. After creating an account on the Twitter developer site, you can obtain these credentials by [creating an "application"](https://dev.twitter.com/apps/new). After you've created an application, create an access token by clicking on the button "Create my access token".

In order to run `GatherStatusStream`, you must save your Twitter API OAuth credentials in a file named `twitter4j.properties` in your current working directory. See [this page](http://twitter4j.org/en/configuration.html) for more information about Twitter4j configurations. The file should contain the following (replace the `**********` instances with your information):

    oauth.consumerKey=**********
    oauth.consumerSecret=**********
    oauth.accessToken=**********
    oauth.accessTokenSecret=**********

Once you have created the `twitter4j.properties` file, you can begin sampling from the public stream using the following invocation:

    etc/run.sh cc.twittertools.stream.GatherStatusStream

The tool will download JSON statuses continuously until it is stopped. Statuses will be saved in the current working directory and compressed hourly. It is recommended that you run the crawler from a server that has good network connections. Crawling from EC2 is a good choice.

As an example of what you'd expected in a crawl, consider data from 2013/01/23 (times in UTC):

     $ du -h statuses.log.2013-01-23-*
     79M	statuses.log.2013-01-23-00.gz
     84M	statuses.log.2013-01-23-01.gz
     87M	statuses.log.2013-01-23-02.gz
     90M	statuses.log.2013-01-23-03.gz
     78M	statuses.log.2013-01-23-04.gz
     64M	statuses.log.2013-01-23-05.gz
     54M	statuses.log.2013-01-23-06.gz
     50M	statuses.log.2013-01-23-07.gz
     48M	statuses.log.2013-01-23-08.gz
     50M	statuses.log.2013-01-23-09.gz
     57M	statuses.log.2013-01-23-10.gz
     68M	statuses.log.2013-01-23-11.gz
     80M	statuses.log.2013-01-23-12.gz
     89M	statuses.log.2013-01-23-13.gz
     96M	statuses.log.2013-01-23-14.gz
     93M	statuses.log.2013-01-23-15.gz
     85M	statuses.log.2013-01-23-16.gz
     77M	statuses.log.2013-01-23-17.gz
     73M	statuses.log.2013-01-23-18.gz
     72M	statuses.log.2013-01-23-19.gz
     79M	statuses.log.2013-01-23-20.gz
     87M	statuses.log.2013-01-23-21.gz
     88M	statuses.log.2013-01-23-22.gz
     84M	statuses.log.2013-01-23-23.gz

Here are more details:

     2013-01-23	00	213981	22021
     2013-01-23	01	226296	20615
     2013-01-23	02	232266	21520
     2013-01-23	03	240487	21694
     2013-01-23	04	211955	22423
     2013-01-23	05	175153	20096
     2013-01-23	06	150733	20564
     2013-01-23	07	132684	15812
     2013-01-23	08	125808	13876
     2013-01-23	09	127156	11929
     2013-01-23	10	143035	12153
     2013-01-23	11	169064	14078
     2013-01-23	12	200296	16107
     2013-01-23	13	222173	17709
     2013-01-23	14	240975	20703
     2013-01-23	15	237227	20556
     2013-01-23	16	222692	22860
     2013-01-23	17	205008	20898
     2013-01-23	18	196170	22187
     2013-01-23	19	197398	23250
     2013-01-23	20	210420	21005
     2013-01-23	21	228628	20463
     2013-01-23	22	232572	25613
     2013-01-23	23	219770	19348

The second column is the hour (in UTC), the third column is the number of JSON messages, and the fourth column is the number of deletes.

**Notes:**

* For the official corpus to be used in the TREC 2013 microblog evaluation, crawling the public stream sample will commence on 2013/02/01 00:00:00 UTC and continue for the entire month of February and March, ending 2013/03/31 23:59:59 UTC.

* Not all JSON messages returned by the API correspond to tweets. In particular, some messages correspond to deleted tweets. See the [Twitter Streaming API page](https://dev.twitter.com/docs/streaming-apis/messages#Status_deletion_notices_delete) for details.


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
