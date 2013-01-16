Twitter Tools
=============

** version 1.0.0 **

These tools associated with the tweets corpus prepared for the [TREC Microblog Track](https://sites.google.com/site/microblogtrack/). The mailing list is [ trec-microblog@googlegroups.com](http://groups.google.com/group/trec-microblog).

The corpus is distributed as directories, each of which contains approximately 100 `.dat` files, each of which contains a list of (tweet id, username, MD5 checksum). Each of these files is referred to as a status block (i.e., block of tweets).

Once you've cloned the repository, you should be able to type `ant` to build the tools (assuming you have Ant installed). Ant will automatically create a script `etc/run.sh` that will be useful later.

The corpus downloader works by twitter.com. The combination of tweet id and username straightforwardly maps to a URL, which can be retrieved. Think `curl`. On steroids.

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
