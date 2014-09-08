package cc.twittertools.hbase;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.eclipse.jdt.core.dom.ThisExpression;
import org.jcodings.util.ArrayCopy;

import antlr.ByteBuffer;


public class WordCountDAO {
	 public static int NUM_INTERVALS = 288;
	 public static final byte[] TABLE_NAME = Bytes.toBytes("wordcount");
	 public static final byte[] COLUMN_FAMILY = Bytes.toBytes("count");
	 
	 //FastPFor compression initialization
	 private static FastPFOR p4 = new FastPFOR();
	 private static int blocks = NUM_INTERVALS / 128; // block number of FastPFor compression
	 private static int left = NUM_INTERVALS % 128; // left number not be compressed
	 
	 private static final Logger log = Logger.getLogger(WordCountDAO.class);

	 private HTablePool pool;
	 
	 public WordCountDAO(HTablePool pool) {
		 this.pool = pool;
	 }
	 
	 public void CreateTable() throws IOException, ZooKeeperConnectionException{
		 Configuration conf = HBaseConfiguration.create();
		  
		 HBaseAdmin hbase = new HBaseAdmin(conf);
		 HTableDescriptor[] wordcounts = hbase.listTables("wordcount");
		  
		 if(wordcounts.length != 0){ //Drop Table if Exists
		 	 hbase.disableTable(TABLE_NAME);
			 hbase.deleteTable(TABLE_NAME);
		 }
		 
		 HTableDescriptor wordcount = new HTableDescriptor(TABLE_NAME);
		 hbase.createTable(wordcount);
		 // Cannot edit a stucture on an active table.
		 hbase.disableTable(TABLE_NAME);
		 HColumnDescriptor columnFamily = new HColumnDescriptor(COLUMN_FAMILY);
		 hbase.addColumn(TABLE_NAME, columnFamily);
		 hbase.enableTable(TABLE_NAME);
		 
		 hbase.close();
	 }
	 
	 private static Get mkGet(String word) throws IOException {
		 log.debug(String.format("Creating Get for %s", word));

		 Get g = new Get(Bytes.toBytes(word));
		 g.addFamily(COLUMN_FAMILY);
		 return g;
	 }
	 
	 private static Put mkPut(WordCount w){
		 log.debug(String.format("Creating Put for %s", w.word));

		 Put p = new Put(Bytes.toBytes(w.word));
		 // add integer compression here
		 // convert 2-d byte array to 1-d byte array
		 int[] out = new int[blocks*128];
		 IntWrapper inPos = new IntWrapper(0);
		 IntWrapper outPos = new IntWrapper(0);
		 p4.compress(w.count, inPos, blocks*128, out, outPos);
		 
		 // the first blocks*128 integers(outPos.get()) are compressed, while the left remains the same
		 int[] compression = new int[outPos.get()+left];
		 // copy the compressed integers
		 System.arraycopy(out, 0, compression, 0, outPos.get()); 
		 // copy the uncompressed integers
		 System.arraycopy(w.count, blocks*128, compression, outPos.get(), left); 
		 
		 byte[] storage = ArrayCopy.int2byte(compression);
		 p.add(COLUMN_FAMILY, Bytes.toBytes(w.column_id), storage);
		 
		 return p;
	 }
	 
	 private static Delete mkDel(String word) {
	    log.debug(String.format("Creating Delete for %s", word));

	    Delete d = new Delete(Bytes.toBytes(word));
	    return d;
	  }

	 private static Scan mkScan() {
		 Scan s = new Scan();
		 s.addFamily(COLUMN_FAMILY);
		 return s;
	 }
	 
	 public void addWordCount(WordCount w) throws IOException{
		 HTableInterface words = pool.getTable(TABLE_NAME);
		 Put p = mkPut(w);
		 words.put(p);
		 words.close();
	 }
	 
	 public List<WordCount> getWordCount(String word) throws IOException {
		 HTableInterface words = pool.getTable(TABLE_NAME);
		 Get g = mkGet(word);
		 Result result = words.get(g);
		 if (result.isEmpty()) {
		      log.info(String.format("word %s not found.", word));
		      return null;
		 }

		 List<WordCount> wordCounts = WordCount.GetWordCountFromResults(result);
		 words.close();
		 return wordCounts;
	 }
	 
	 public void deleteUser(String word) throws IOException {
		 HTableInterface words = pool.getTable(TABLE_NAME);

	     Delete d = mkDel(word);
	     words.delete(d);

	     words.close();
	 }
	 
	 public static class WordCount{
		 public String word;
		 public String column_id;
		 public int[] count;
		 public int sum;
		 
		 public WordCount(String word){
			 this.word = word;
			 this.sum = 0;
			 this.count = new int[NUM_INTERVALS];
			 for(int i=0; i < NUM_INTERVALS; i++){
				 this.count[i] = 0;
			 }
		 }
		 
		 public WordCount(byte[] word, byte[] column_id){
			 this.word = new String(word);
			 this.column_id = new String(column_id);
			 this.sum = 0;
			 this.count = new int[NUM_INTERVALS];
			 for(int i=0; i < NUM_INTERVALS; i++){
				 this.count[i] = 0;
			 }
		 }
		 
		 public WordCount(String word, String column_id){
			 this.word = word;
			 this.column_id = column_id;
			 this.sum = 0;
			 this.count = new int[NUM_INTERVALS];
			 for(int i=0; i < NUM_INTERVALS; i++){
				 this.count[i] = 0;
			 }
		 }
		 
		 public WordCount(String word, String column_id, int[] count){
			 this.word = word;
			 this.column_id = column_id;
			 this.sum = 0;
			 this.count = new int[count.length];
			 for(int i=0; i < count.length; i++){
				 this.count[i] = count[i];
				 sum += this.count[i];
			 }
		 }
		 
		 public static List<WordCount> GetWordCountFromResults(Result r){
			 List<WordCount> wordCounts = new ArrayList<WordCount>();
			 byte[] word = r.getRow();
			 // Map from column qualifiers to values
			 NavigableMap<byte[],byte[]> familyMap = r.getFamilyMap(COLUMN_FAMILY);
			 for(byte[] column: familyMap.keySet()){
				 byte[] value = familyMap.get(column);
				 // decompression
				 // convert byte array to int array
				 IntBuffer intBuffer = java.nio.ByteBuffer.wrap(value)
						 .order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
				 int[] rawInts = new int[intBuffer.remaining()];
				 intBuffer.get(rawInts);
				 
				 int[] count = new int[NUM_INTERVALS];
				 IntWrapper inPos = new IntWrapper(0);
				 IntWrapper outPos = new IntWrapper(0);
				 p4.uncompress(rawInts, inPos, rawInts.length, count, outPos);
				 System.arraycopy(rawInts, rawInts.length-left, count, NUM_INTERVALS-left, left);
				 
				 WordCount w = new WordCount(new String(word), new String(column), count);
				 
				 wordCounts.add(w);
			 }
			 return wordCounts;
		 }
		 
		 public void setCount(int interval, int count){
			 this.count[interval] = count;
			 this.sum += count;
		 }
		 
		 public int getSum(){
			 return sum;
		 }
		 
		 @Override
		 public boolean equals(Object o){
			 if (!(o instanceof WordCount)) {
				 return false;
			 }
			 if (o == this) {
				 return true;
			 }
			 WordCount w = (WordCount)o;
			 if(w.word.equals(this.word)  &&
				w.column_id.equals(this.column_id)) {
				 for(int i=0; i<NUM_INTERVALS; i++){
					 if(w.count[i] != this.count[i]){
						 return false;
					 }
				 }
				 return true;
			 }
			 return false;
		 }
	 }
}
