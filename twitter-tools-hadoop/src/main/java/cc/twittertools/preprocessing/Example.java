package cc.twittertools.preprocessing;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

import cc.twittertools.index.LowerCaseEntityPreservingFilter;

public class Example {
	static String WORD;
	public static void main(String[] args) throws Exception{
		Table<String, Integer, Integer> map = HashBasedTable.create();
		IntArrayList intList = new IntArrayList(new int[]{1,2,3});
		ByteArrayList byteList = new ByteArrayList(new byte[]{0x08,0x04,0x02,0x01});
		ShortArrayList shortList = new ShortArrayList(new short[]{4,5,6,7});
		map.put("a", 1, 0);
		map.put("a", 2, 0);
		map.put("aa", 1, 2);
		map.put("aaa", 2, 3);
		
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("data.out"));
		out.writeObject(map);
		out.writeObject(intList);
		out.writeObject(byteList);
		out.writeObject(shortList);
		out.close();
		
		ObjectInputStream in = new ObjectInputStream(new FileInputStream("data.out"));
		Table<String, Integer, Integer> map2 = (Table<String, Integer, Integer>)in.readObject();
		IntArrayList intList2 = (IntArrayList) in.readObject();
		ByteArrayList byteList2 = (ByteArrayList) in.readObject();
		ShortArrayList shortList2 = (ShortArrayList) in.readObject();
		
		for(Cell<String, Integer, Integer> c : map2.cellSet()) {
			System.out.println(c.getRowKey()+":"+c.getColumnKey()+":"+c.getValue());
		}
		for(int unit : intList2) {
			System.out.print(unit + " ");
		}
		for(byte unit: byteList2) {
			System.out.print(unit + " ");
		}
		for(short unit: shortList2) {
			System.out.print(unit + " ");
		}
		
		/*String str = "Mon Apr 01 23:59:59 +0000 2014";
		Date base = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").parse("2013-04-01 00:00:00 +0000");
		Date current = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy").parse(str);
		long diff = current.getTime() - base.getTime();
		System.out.println ("Days: " + TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS));
		//Test GetInterval Correctness
		WORD = args[0];
		String wordDir = args[1];
		String path = args[2];
		Table<String, Integer, Integer> freqTable = HashBasedTable.create();
		BufferedReader bf = new BufferedReader(new FileReader(wordDir));
		String line;
		while((line = bf.readLine()) != null){
			String[] groups = line.split(",");
			int freq = Integer.parseInt(groups[3]);
			String[] times = groups[1].split("\\s+");
			String day = times[2] + " " + times[0] + " " + times[1];
			String time = times[3];
			String[] timeGroups= time.split(":");
			int interval = (Integer.valueOf(timeGroups[0]))*12 + (Integer.valueOf(timeGroups[1])/5);
			if(!freqTable.contains(day, interval)){
				freqTable.put(day, interval, freq);
			}else{
				int tmpFreq = freqTable.get(day, interval);
				freqTable.put(day, interval, tmpFreq+freq);
			}
		}
		BufferedWriter bw = new BufferedWriter(new FileWriter("world-interval.txt"));
		for(Cell<String, Integer, Integer> c: freqTable.cellSet()){
			bw.write(c.getRowKey() + "   " + c.getColumnKey() + " " +c.getValue() + "\n");
		}
		bw.close();
		
		File folder = new File(path);
		if(folder.isDirectory()){
			for (File file : folder.listFiles()) {
				if(!file.getName().startsWith("part"))
					continue;
				String filePath = path+file.getName();
				BufferedReader bf2 = new BufferedReader(new FileReader(filePath));
				while((line=bf2.readLine())!=null){
					String[] groups = line.split("\\t");
					if(groups.length != 4) 
						continue;
					String word = groups[0]; // each day is viewed as a column in underlying HBase
					String day = groups[1];
					int interval = Integer.parseInt(groups[2]);
					int count = Integer.parseInt(groups[3]);
					if(word.equals(WORD)){
						try{
							int indexCount = freqTable.get(day, interval);
							if(count != indexCount){
								System.out.println("Incorrect" + day + " " + interval + ":" + count + "," +indexCount);
							}else{
								System.out.println("Correct" + day + " " + interval + ":" + count + "," +indexCount);
							}
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				}
			}
		}*/
	}
}
