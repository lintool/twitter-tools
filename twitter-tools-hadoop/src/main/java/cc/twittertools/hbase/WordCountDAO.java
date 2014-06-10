package cc.twittertools.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

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


public class WordCountDAO {
	 private final static int DAY = 60*24;
	 private final static int INTERVAL = 5;
	 public static int NUM_INTERVALS = DAY/INTERVAL;
	 public static final byte[] TABLE_NAME = Bytes.toBytes("wordcount");
	 public static final byte[] COLUMN_FAMILY = Bytes.toBytes("count");
	 
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

		 Put p = new Put(w.word);
		 // add integer compression here
		 // convert 2-d byte array to 1-d byte array
		 byte[] storage = new byte[NUM_INTERVALS*Integer.SIZE/Byte.SIZE];
		 for(int i=0; i< NUM_INTERVALS; i++){
			 for(int j=0; j<Integer.SIZE/Byte.SIZE; j++){
				storage[i*Integer.SIZE/Byte.SIZE+j] = w.count[i][j]; 
			 }
		 }
		 p.add(COLUMN_FAMILY, w.column_id, storage);
		 
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
		 public byte[] word;
		 public byte[] column_id;
		 public byte[][] count;
		 
		 public WordCount(byte[] word, byte[] column_id){
			 this.word = word;
			 this.column_id = column_id;
			 this.count = new byte[NUM_INTERVALS][];
			 for(int i=0; i < NUM_INTERVALS; i++){
				 this.count[i] = Bytes.toBytes(0);
			 }
		 }
		 
		 public WordCount(String word, String column_id){
			 this.word = Bytes.toBytes(word);
			 this.column_id = Bytes.toBytes(column_id);
			 this.count = new byte[NUM_INTERVALS][];
			 for(int i=0; i < NUM_INTERVALS; i++){
				 this.count[i] = Bytes.toBytes(0);
			 }
		 }
		 
		 private WordCount(byte[] word, byte[] column_id, byte[][] count){
			 this.word = word;
			 this.column_id = column_id;
			 this.count = count;
		 }
		 
		 public static List<WordCount> GetWordCountFromResults(Result r){
			 List<WordCount> wordCounts = new ArrayList<WordCount>();
			 byte[] word = r.getRow();
			 // Map from column qualifiers to values
			 NavigableMap<byte[],byte[]> familyMap = r.getFamilyMap(COLUMN_FAMILY);
			 for(byte[] column: familyMap.keySet()){
				 byte[] value = familyMap.get(column);
				 // decompression
				 byte[][] count = new byte[NUM_INTERVALS][Integer.SIZE/Byte.SIZE];
				 for(int i=0; i<NUM_INTERVALS; i++){
					 for(int j=0; j<Integer.SIZE/Byte.SIZE; j++){
						 count[i][j] = value[i*Integer.SIZE/Byte.SIZE+j];
					 }
				 }
				 WordCount w = new WordCount(word, column, count);
				 wordCounts.add(w);
			 }
			 return wordCounts;
		 }
		 
		 public void setCount(int interval, int count){
			 this.count[interval] = Bytes.toBytes(count);
		 }
	 }
}
