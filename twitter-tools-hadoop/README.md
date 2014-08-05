# Analyzing Tweets with Pig: Getting Started

Since tweets are encoded in JSON, and Pig offers poor native JSON support, it's more convenient to use JSON loaders in Twitter's [Elephant Bird](https://github.com/kevinweil/elephant-bird/) library. Easiest just to fetch the relevant jars directly:

```
wget http://repo1.maven.org/maven2/com/twitter/elephantbird/elephant-bird-core/4.5/elephant-bird-core-4.5.jar
wget http://repo1.maven.org/maven2/com/twitter/elephantbird/elephant-bird-pig/4.5/elephant-bird-pig-4.5.jar
wget http://repo1.maven.org/maven2/com/twitter/elephantbird/elephant-bird-hadoop-compat/4.5/elephant-bird-hadoop-compat-4.5.jar
wget http://repo1.maven.org/maven2/com/googlecode/json-simple/json-simple/1.1.1/json-simple-1.1.1.jar
```

You're ready to start analyzing tweets with Pig! Here's the obligatory word count example in Pig:

```
register 'elephant-bird-core-4.5.jar';
register 'elephant-bird-pig-4.5.jar';
register 'elephant-bird-hadoop-compat-4.5.jar';
register 'json-simple-1.1.1.jar';

raw = load '/path/to/tweets' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

a = foreach raw generate (chararray) $0#'text' as text;
b = foreach a generate flatten(TOKENIZE(text)) as word;
c = group b by word;
d = foreach c generate COUNT(b), group;

store d into 'wordcount';
```
