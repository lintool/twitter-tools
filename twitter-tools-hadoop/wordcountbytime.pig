register 'jar/elephant-bird-core-4.5.jar';
register 'jar/elephant-bird-pig-4.5.jar';
register 'jar/elephant-bird-hadoop-compat-4.5.jar';
register 'jar/json-simple-1.1.1.jar';
register 'jar/twitter-tools-hadoop-1.0-SNAPSHOT.jar';
register 'jar/twitter-tools-core-1.4.3-SNAPSHOT.jar'; 
register 'jar/lucene-core-4.8.0.jar';
register 'jar/lucene-analyzers-common-4.8.0.jar';
register 'jar/twitter-text-1.9.0.jar';

raw = load '/shared/collections/Tweets2011/20110208-099.json.gz' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

a = foreach raw generate $0#'created_at',$0#'text';
b = foreach a generate cc.twittertools.udf.GetDate($0), cc.twittertools.udf.GetInterval($0), flatten(cc.twittertools.udf.LuceneTokenizer($1));
c = group b by ($0,$1,$2);
d = foreach c generate flatten(group),COUNT(b);

store d into 'wordcount';
