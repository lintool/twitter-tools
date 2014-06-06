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
	 private static int NUM_INTERVALS = DAY/INTERVAL;
	 public static final byte[] TABLE_NAME = Bytes.toBytes("wordcount");
	 
	 public static final byte[][] COUNT_COLS = new byte[NUM_INTERVALS][Integer.SIZE];
	 
	 private static final Logger log = Logger.getLogger(WordCountDAO.class);

	 private HTablePool pool;
	 
	 public WordCountDAO(HTablePool pool) {
		 this.pool = pool;
		 for(int i=0; i<NUM_INTERVALS; i++){
			 COUNT_COLS[i] = Bytes.toBytes("i"+i);
		 }
	 }
	 
	 public void CreateTable(Set<String> familySet) throws IOException, ZooKeeperConnectionException{
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
		 for(String family : familySet){
			 HColumnDescriptor columnFamily = new HColumnDescriptor(family.getBytes());
			 hbase.addColumn(TABLE_NAME, columnFamily);
		 }
		 hbase.enableTable(TABLE_NAME);
		 
		 hbase.close();
	 }
	 
	 private static Get mkGet(String word) throws IOException {
		 log.debug(String.format("Creating Get for %s", word));

		 Get g = new Get(Bytes.toBytes(word));
		 return g;
	 }
	 
	 private static Get mkGetByFamily(String word, String family_id) throws IOException {
		 log.debug(String.format("Creating Get for %s", word));

		 Get g = new Get(Bytes.toBytes(word));
		 g.addFamily(Bytes.toBytes(family_id));
		 return g;
	 }
	 
	 private static Put mkPut(WordCount w){
		 log.debug(String.format("Creating Put for %s", w.word));

		 Put p = new Put(w.word);
		 byte[] time_family = w.family_id;
		 for(byte[] column : w.countMap.keySet()){
			 p.add(time_family, column, w.countMap.get(column));
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
		 return s;
	 }
	 
	 private static Scan mkScanByFamily(String family_id) {
		 Scan s = new Scan();
		 s.addFamily(Bytes.toBytes(family_id));
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
		 public byte[] family_id;
		 public NavigableMap<byte[],byte[]> countMap;
		 private static Comparator<byte[]> comparator = new Comparator<byte[]>(){
			 @Override
			 public int compare(byte[] b1, byte[] b2){
				return b1.toString().compareTo(b2.toString()); 
			 }
		 };
		 
		 public WordCount(byte[] word, byte[] family_id){
			 this.word = word;
			 this.family_id = family_id;
			 this.countMap = new TreeMap<byte[],byte[]>(comparator);
			 
		 }
		 
		 public WordCount(String word, String family_id){
			 this.word = Bytes.toBytes(word);
			 this.family_id = Bytes.toBytes(family_id);
			 this.countMap = new TreeMap<byte[],byte[]>(comparator);
		 }
		 
		 private WordCount(String word, String family_id, NavigableMap<byte[], byte[]> countMap) {
			this.word = Bytes.toBytes(word);
			this.family_id = Bytes.toBytes(family_id);
			this.countMap = countMap;
		 }
		 
		 private WordCount(byte[] word, byte[] family_id, NavigableMap<byte[], byte[]> countMap){
			 this.word = word;
			 this.family_id = family_id;
			 this.countMap = countMap;
		 }
		 
		 public static List<WordCount> GetWordCountFromResults(Result r){
			 List<WordCount> wordCounts = new ArrayList<WordCount>();
			 byte[] word = r.getRow();
			 NavigableMap<byte[],NavigableMap<byte[],byte[]>> familyMaps = r.getNoVersionMap();
			 for(byte[] family: familyMaps.keySet()){
				 NavigableMap map = familyMaps.get(family);
				 WordCount w = new WordCount(word, family, map);
				 wordCounts.add(w);
			 }
			 return wordCounts;
		 }
		 
		 public void setCount(String column, String count){
			 this.countMap.put(Bytes.toBytes(column), Bytes.toBytes(count));
		 }
	 }
}
