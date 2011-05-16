Reading the Corpus
---------------------------

We've provided a few reference implementations illustrating how to read the corpus, in package `com.twitter.corpus.demo`.  `ReadStatuses` provides an example of how to read a block of statuses (tweets).

Example: read block `foo/bar.gz`, dump statuses to stdout:

     java -cp lib/*:dist/twitter-corpus-tools-0.0.1.jar com.twitter.corpus.demo.ReadStatuses \
        -input foo/bar.gz -dump

Read all blocks in directory `foo`:

    java -cp lib/*:dist/twitter-corpus-tools-0.0.1.jar com.twitter.corpus.demo.ReadStatuses \
        -input foo -verbose

Indexing the Corpus
---------------------------

We've also provided a reference implementation of using Lucene to index the statuses:

    java -cp lib/*:dist/twitter-corpus-tools-0.0.1.jar com.twitter.corpus.demo.IndexStatuses \
        -input corpus -index index

Simple program to search the built index:

    java -cp lib/*:dist/twitter-corpus-tools-0.0.1.jar com.twitter.corpus.demo.SearchStatuses \
    -index index -query egypt -num_hits 100
