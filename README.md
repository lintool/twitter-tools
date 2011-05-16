twitter-corpus-tools
====================

These tools associated with the tweets corpus prepared for the [TREC 2011 Microblog Task](https://sites.google.com/site/microblogtrack/). The mailing list is [ trec-microblog@googlegroups.com](http://groups.google.com/group/trec-microblog).

The corpus is distributed as directories, each of which contains approximately 100 `.dat` files, each of which contains a list of (tweet id, screen name, MD5 checksum). Each of these files is referred to as a status block (i.e., block of tweets).

Once you've cloned the repository, you should be able to type `ant` to build the tools.

Fetching a Status Block
-----------------------

We've provided a crawler for fetching a status block. Here's a sample invocation:

    java -cp 'lib/*:dist/twitter-corpus-tools-0.0.1.jar' com.twitter.corpus.download.AsyncStatusBlockFetcher \
       -data 20110123/20110123-000.dat -output json/20110123-000.json.gz

Use the `-data` option to specify the status block (`.dat` file) to read. Use the `-output` option to specify where to write the output (gzipped JSON-encoded statuses, one per line).

You'll need to fetch all blocks in the corpus using `com.twitter.corpus.download.AsyncStatusBlockFetcher`. It makes sense to do some lightweight scripting (e.g., shell scripts, Python, Perl, etc.) to accomplish this. We have decided not to include these scripts because 1.) they are easy enough to write, and 2.) most likely, you may wish to download the blocks in parallel from multiple machines, in which no general purpose script would be particularly useful.

**Tip:** The fetcher may not succeed in downloading all tweets in a particular block. Don't worry about that for now: we're in the process of writing a "verification" tool that will allow you to repair downloaded status blocks and refetch missing statuses.

Reading the Corpus
------------------

We've provided a few reference implementations illustrating how to read the corpus, once you downloaded the gzipped JSON-encoded statuses. Tools are in package `com.twitter.corpus.demo`.  `ReadStatuses` provides an example of how to read the statuses.

Example: read block `foo/bar.gz`, dump statuses to stdout:

     java -cp lib/*:dist/twitter-corpus-tools-0.0.1.jar com.twitter.corpus.demo.ReadStatuses \
        -input foo/bar.gz -dump

Read all blocks in directory `foo/`:

    java -cp lib/*:dist/twitter-corpus-tools-0.0.1.jar com.twitter.corpus.demo.ReadStatuses \
        -input foo -verbose

Searching the Corpus
--------------------

We've also provided a reference implementation of using Lucene to index the statuses:

    java -cp lib/*:dist/twitter-corpus-tools-0.0.1.jar com.twitter.corpus.demo.IndexStatuses \
        -input corpus -index index

Simple program to search the built index:

    java -cp lib/*:dist/twitter-corpus-tools-0.0.1.jar com.twitter.corpus.demo.SearchStatuses \
    -index index -query egypt -num_hits 100
