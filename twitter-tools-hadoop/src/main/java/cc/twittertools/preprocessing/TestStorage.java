package cc.twittertools.preprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.thrift.generated.Hbase.isTableEnabled_args;
import org.jcodings.util.ArrayCopy;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import sun.security.util.BigInt;
import cc.twittertools.hbase.WordCountDAO;
import cc.twittertools.hbase.WordCountDAO.WordCount;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;

public class TestStorage {
	private static final Random RANDOM = new Random();
	
	public static void testPFor1() throws Exception {
		int len = 288;
	    FastPFOR p4 = new FastPFOR();
	    int[] doc = new int[len];
	    for (int i = 0; i<len; i++) {
	      doc[i] = RANDOM.nextInt(10000);
	    }
	
	     int blocks = len / 128; // block number of FastPFor compression
		 int left = len % 128;
		 
		 IntWrapper inPos = new IntWrapper(0);
		 IntWrapper outPos = new IntWrapper(0);
		 int[] out = new int[blocks*128];
		 p4.compress(doc, inPos, blocks*128, out, outPos);
		 
		 // the first blocks*128 integers are compressed, while the left remains the same
		 int[] compression = new int[outPos.get()+left];
		 // copy the compressed integers
		 System.arraycopy(out, 0, compression, 0, outPos.get()); 
		 // copy the uncompressed integers
		 System.arraycopy(doc, blocks*128, compression, outPos.get(), left); 
		 
		 byte[] storage = ArrayCopy.int2byte(compression);
		 
		 IntBuffer intBuffer = java.nio.ByteBuffer.wrap(storage)
				 .order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
		 int[] rawInts = new int[intBuffer.remaining()];
		 intBuffer.get(rawInts);
		 
		 int[] count = new int[len];
		 IntWrapper cinPos = new IntWrapper(0);
		 IntWrapper coutPos = new IntWrapper(0);
		 p4.uncompress(rawInts, cinPos, rawInts.length, count, coutPos);
		 System.arraycopy(rawInts, rawInts.length-left, count, len-left, left);
	    
	    for (int i = 0; i < doc.length; i++) {
	      if(doc[i] != count[i]){
	    	  System.out.println(i);
	      }
		}
	}
	
	public static Table<String, String, WordCountDAO.WordCount> LoadWordCount(String args) throws IOException{
		Table<String, String, WordCountDAO.WordCount> wordCountMap = HashBasedTable.create();
		File folder = new File(args);
		if(folder.isDirectory()){
			for (File file : folder.listFiles()) {
				if(!file.getName().startsWith("part"))
					continue;
				System.out.println("Processing "+args+file.getName());
				BufferedReader bf = new BufferedReader(new FileReader(args+file.getName()));
				// each line in wordcount file is like : 1 twitter 100
				String line;
				while((line=bf.readLine())!=null){
					String[] groups = line.split("\\t");
					if(groups.length != 4) 
						continue;
					String day = groups[0]; // each day is viewed as a column in underlying HBase
					String interval = groups[1];
					String word = groups[2];
					String count = groups[3];
					if(!wordCountMap.contains(word, day)){
						WordCountDAO.WordCount w = new WordCountDAO.WordCount(word, day);
						wordCountMap.put(word, day, w);
					}
					WordCountDAO.WordCount w = wordCountMap.get(word, day);
					w.setCount(Integer.valueOf(interval), Integer.valueOf(count));
					wordCountMap.put(word, day, w);
					
				}
			}
		}
		return wordCountMap;
	}
	public static void TestStorage() throws IOException{
		HTablePool pool = new HTablePool();
		WordCountDAO DAO = new WordCountDAO(pool);
		String[] words = {"a", "1", "haha", "stream", "champions"};
		Table<String, String, WordCountDAO.WordCount> wordCountMap = LoadWordCount("wordcount/");
		for(String word: words){
			List<WordCount> list = DAO.getWordCount(word);
			for(WordCount storageW : list){
				String column = new String(storageW.column_id);
				WordCountDAO.WordCount sourceW = wordCountMap.get(word, column);
				for(int i=0; i<WordCountDAO.NUM_INTERVALS; i++){
					if(sourceW.count[i] != storageW.count[i]){
						System.out.println(word+","+column+","+i+":"+sourceW.count[i]+","+storageW.count[i]);
					}
				}
			}
		}
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		TestStorage();
	}

}
