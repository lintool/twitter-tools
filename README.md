Twitter Tools
=============

This is a collection of tools for the TREC Microblog Track, which contains the official search API for TREC 2014 (which was also used in TREC 2013). Please join the mailing list for discussion at [trec-microblog@googlegroups.com](http://groups.google.com/group/trec-microblog).

Participating in TREC 2014
--------------------------

In order to participate in the TREC 2014 Microblog track, you need to register to participate in TREC. See the [call for participation](http://trec.nist.gov/pubs/call2014.html). The call will close in late May.

The Microblog track in 2014 will use the "evaluation as a service" (EaaS) model (more below), where teams interact with the official corpus via a common API. Thus, you need to request access to the API via the following steps:

**NOTE**: If you participated in TREC 2013 and already have access to the API, you do not need to do anything.

1. Fill out the [API usage agreement](http://lintool.github.io/twitter-tools/API-agreement.pdf).
2. Email the usage agreement to `microblog-request@nist.gov`.
3. After NIST receives your request, you will receive an access token from NIST.
4. The code for accessing the API can be found in this repository. The endpoint of API itself (i.e., hostname, port) can be found at [this location](http://www.umiacs.umd.edu/~jimmylin/trec2014microblog/servers.txt).

Note that the file is password protected with the same username/password combination as the TREC 2014 Active Participants site: you should have received the username/password when you signed up for TREC 2014. Please do not publicize this information. 

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

To automatically generate project files for Eclipse:

```
$ mvn eclipse:clean
$ mvn eclipse:eclipse
```

You can then use Eclipse's Import "Existing Projects into Workspace" functionality to import the project.

For more information, see the [project wiki](https://github.com/lintool/twitter-tools/wiki).

**Note** that for version 1.3.0 and before, the project used Ant for build management. Starting version 1.4.0, the project switched over to Maven for build management.

License
-------

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0


Acknowledgements
----------------

This work is supported in part by the National Science Foundation under award [IIS-1218043](http://www.nsf.gov/awardsearch/showAward?AWD_ID=1218043). Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the National Science Foundation.
