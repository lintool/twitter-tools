package cc.twittertools.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


public class WordCountDAO {
	 private static int NUM_INTERVALS = 12;
	 public static final byte[] TABLE_NAME = Bytes.toBytes("wordcount");
	 public static final byte[] TIME_FAMILY   = Bytes.toBytes("time");
	 
	 public static final byte[] WORD_COL = Bytes.toBytes("word");
	 public static final byte[][] COUNT_COLS = new byte[NUM_INTERVALS][Integer.SIZE];
	 
	 private static final Logger log = Logger.getLogger(WordCountDAO.class);

	 private HTablePool pool;
	 
	 public WordCountDAO(HTablePool pool) {
		 this.pool = pool;
		 for(int i=0; i<NUM_INTERVALS; i++){
			 COUNT_COLS[i] = Bytes.toBytes("i"+i);
		 }
	 }
	 
	 public void CreateTable(){
		 
	 }
	 
	 private static Get mkGet(String word) throws IOException {
		 log.debug(String.format("Creating Get for %s", word));

		 Get g = new Get(Bytes.toBytes(word));
		 g.addFamily(TIME_FAMILY);
		 return g;
	 }
	 
	 private static Put mkPut(WordCount w){
		 log.debug(String.format("Creating Put for %s", w.word));

		 Put p = new Put(Bytes.toBytes(w.word));
		 p.add(TIME_FAMILY,WORD_COL,Bytes.toBytes(w.word));
		 for(int i=0; i<NUM_INTERVALS; i++){
			 p.add(TIME_FAMILY, Bytes.toBytes("i"+i), Bytes.toBytes(w.count[i]));
		 }
		 return p;
	 }
	 
	 private static Delete mkDel(String word) {
	    log.debug(String.format("Creating Delete for %s", word));

	    Delete d = new Delete(Bytes.toBytes(word));
	    return d;
	  }

	 private static Scan mkScan() {
		 Scan s = new Scan();
		 s.addFamily(TIME_FAMILY);
		 return s;
	 }
	 
	 public void addWordCount(WordCount w) throws IOException{
		 HTableInterface words = pool.getTable(TABLE_NAME);
		 Put p = mkPut(w);
		 words.put(p);
		 words.close();
	 }
	 
	 public WordCount getWordCount(String word) throws IOException {
		 HTableInterface words = pool.getTable(TABLE_NAME);
		 Get g = mkGet(word);
		 Result result = words.get(g);
		 if (result.isEmpty()) {
		      log.info(String.format("word %s not found.", word));
		      return null;
		 }

		 WordCount w = new WordCount(result);
		 words.close();
		 return w;
	 }
	 
	 public void deleteUser(String word) throws IOException {
		 HTableInterface words = pool.getTable(TABLE_NAME);

	     Delete d = mkDel(word);
	     words.delete(d);

	     words.close();
	 }
	 
	 public static class WordCount{
		 public String word;
		 public String count[] = new String[NUM_INTERVALS];
		 
		 public WordCount(String word){
			 this.word = word;
			 for(int i=0; i<NUM_INTERVALS; i++){
				 this.count[i] = "0";
			 }
		 }
		 
		 private WordCount(String word, String[] count) {
			this.word = word;
			this.count = count;
		 }
		 
		 private WordCount(Byte[] word, Byte[][] count){
			 this.word = word.toString();
			 this.count = new String[count.length];
			 for(int i=0; i<count.length; i++){
				 this.count[i] = count[i].toString();
			 }
		 }
		 
		 private WordCount(Result r){
			 this.word = r.getValue(TIME_FAMILY, WORD_COL).toString();
			 this.count = new String[NUM_INTERVALS];
			 for(int i=0; i<NUM_INTERVALS; i++){
				 this.count[i] = r.getValue(TIME_FAMILY, COUNT_COLS[i]).toString();
			 }
		 }
		 
		 public void setCount(int interval, String count){
			 this.count[interval] = count;
		 }
	 }
}
