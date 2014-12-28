microblog-demos
===============

Examples of using the [2013 TREC microblog API](http://twittertools.cc/). Basically clones IndriRunQuery.

Getting Started
--------------

Once you've cloned the repository, build the package with Maven:

```
$ mvn clean package appassembler:assemble
```

Appassembler will automatically generate a launch scripts for:

+ `target/appassembler/bin/RunQueries`: baseline run.  with or without RM3 feedback

To automatically generate project files for Eclipse:

```
$ mvn eclipse:clean
$ mvn eclipse:eclipse
```

You can then use Eclipse's Import "Existing Projects into Workspace" functionality to import the project.


Invoking Sample Runs
--------------------
After building, you can run the sample programs via somthing like this:

```
$ sh ./target/appassembler/bin/RunQueries ./config/params_run.json
```

which will run a simple baseline query likelihood retrieval.  All runnable programs are in ./target/appassembler/bin/ .  Also, all programs take a single argument: a JSON-formatted file that will look something like this:
```
{
"queries"      :  "./data/topics.microblog2012.txt",
"retweet"	   :  true or false <control whether to remove retweets in query expansion>
"host"         :  "<host_name>",
"port"         :  9090,
"num_results"  :  1000,
"fb_docs"      :  0,
"fb_terms"     :  0,
"group"        :  "<your_group_here>",
"token"        :  "<your_token_here>",
"runtag"       :  "<run_tag_here>"
}
```

Hopefully these variables are self-explanatory.  Setting either `fb_docs` or `fb_terms` to 0 gives a run with no feedback.  If both of these
are set >0, pseudo-feedback using RM3 is used.

This repo also produce the lexical & temporal feedback results, you can run it like this:

```
$ sh ./target/appassembler/bin/RunTemporalQueries ./config/params_run.json
```

To run RunTemporalQueries, you need to build twitter-tools-kde repo to a jar file, then add it to the dependency list in pom.xml.

In addition, we add following arguments in config/params_run.json file to control the performance of RunTemporalQueries class: 
```
{
"year"         :  "2011" or "2013",
"model"        :  <temporal model option, can be "kde", "win", "recency">,
"alpha"        :  <alpha value for specific temporal model which obtains best performance during training>
}
```

License
-------

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
