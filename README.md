Twitter Tools
=============

This repo holds a collection of tools for the TREC Microblog tracks, which officially ended in 2015. The track mailing list can be found at [trec-microblog@googlegroups.com](http://groups.google.com/group/trec-microblog).

Archival Documents
------------------

+ [TREC 2013 API Specifications](https://github.com/lintool/twitter-tools/wiki/TREC-2013-API-Specifications)
+ [TREC 2013 Track Guidelines](https://github.com/lintool/twitter-tools/wiki/TREC-2013-Track-Guidelines)
+ [TREC 2014 Track Guidelines](https://github.com/lintool/twitter-tools/wiki/TREC-2014-Track-Guidelines)
+ [TREC 2015 Track Guidelines](https://github.com/lintool/twitter-tools/wiki/TREC-2015-Track-Guidelines)

API Access
----------

The Microblog tracks in 2013 and 2014 used the "evaluation as a service" (EaaS) model, where teams interact with the official corpus via a common API. Although the evaluation has ended, the API is still available for researcher use.

To request access to the API, follow these steps:

1. Fill out the [API usage agreement](http://lintool.github.io/twitter-tools/API-agreement.pdf).
2. Email the usage agreement to `microblog-request@nist.gov`.
3. After NIST receives your request, you will receive an access token from NIST.
4. The code for accessing the API can be found in this repository. The endpoint of API itself (i.e., hostname, port) will be provided by NIST.

Getting Stated
--------------

The main Maven artifact for the TREC Microblog API is `twitter-tools-core`. The latest releases of Maven artifacts are available at [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Ccc.twittertools).

You can clone the repo with the following command:

```
$ git clone git://github.com/lintool/twitter-tools.git
``` 

Once you've cloned the repository, change directory into `twitter-tools-core` and build the package with Maven:

```
$ cd twitter-tools-core
$ mvn clean package appassembler:assemble
```

For more information, see the [project wiki](https://github.com/lintool/twitter-tools/wiki).

Replicating TREC Baselines
--------------------------

One advantage of the TREC Microblog API is that it is possible to deploy a community baseline whose results are replicable by *anyone*. The `raw` results are simply the output of the API unmodified. The `baseline` results are the `raw` results that have been post-processed to remove retweets and break score ties by reverse chronological order (earliest first).

To run the `raw` results for TREC 2011, issue the following command:

```
sh target/appassembler/bin/RunQueriesThrift \
 -host [host] -port [port] -group [group] -token [token] \
 -queries ../data/topics.microblog2011.txt > run.microblog2011.raw.txt
```

And to run the `baseline` results for TREC 2011, issue the following command:

```
sh target/appassembler/bin/RunQueriesBaselineThrift \
 -host [host] -port [port] -group [group] -token [token] \
 -queries ../data/topics.microblog2011.txt > run.microblog2011.baseline.txt
```

Note that `trec_eval` is included in `twitter-tools/etc` (just needs to be compiled), and the qrels are stored in `twitter-tools/data` (just needs to be uncompressed), so you can evaluate as follows:

```
../etc/trec_eval.9.0/trec_eval ../data/qrels.microblog2011.txt run.microblog2011.raw.txt
```

Similar commands will allow you to replicate runs for TREC 2012 and TREC 2013. With `trec_eval`, you should get *exactly* the following results:

MAP       | raw    | baseline
----------|--------|---------
TREC 2011 | 0.3050 | 0.3576
TREC 2012 | 0.1751 | 0.2091
TREC 2013 | 0.2044 | 0.2532
TREC 2014 | 0.3090 | 0.3924

P30       | raw    | baseline
----------|--------|---------
TREC 2011 | 0.3483 | 0.4000
TREC 2012 | 0.2831 | 0.3311
TREC 2013 | 0.3761 | 0.4450
TREC 2014 | 0.5145 | 0.6182


License
-------

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).


Acknowledgments
---------------

This work is supported in part by the National Science Foundation under award [IIS-1218043](http://www.nsf.gov/awardsearch/showAward?AWD_ID=1218043). Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the National Science Foundation.
