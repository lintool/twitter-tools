Twitter Tools
=============

This is a collection of tools for the TREC Microblog Track, which contains the official search API for TREC 2013. Please join the mailing list for discussion at [trec-microblog@googlegroups.com](http://groups.google.com/group/trec-microblog).

Currently, the project has a single Maven artifact `twitter-tools-core`. The latest releases of Maven artifacts are available at [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Ccc.twittertools).

Getting Stated
--------------

You can clone the repo with the following command:

```
$ git clone git://github.com/lintool/twitter-tools.git
``` 

Once you've cloned the repository, change directory into `twitter-tools-core` and build the package with Maven:

```
$ cd twitter-tools-core
$ mvn clean package
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
