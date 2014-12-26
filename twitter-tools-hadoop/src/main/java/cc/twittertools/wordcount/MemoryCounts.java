package cc.twittertools.wordcount;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class MemoryCounts {
	// Term ID Table: look up by word(string) and day(integer)
	public Table<String, Integer, Integer> termIdTable;
	public IntArrayList offset;
	public ShortArrayList length;
	public ByteArrayList data;
	
	public MemoryCounts() {
		termIdTable = HashBasedTable.create();
		offset = new IntArrayList();
		length = new ShortArrayList();
		data = new ByteArrayList();
	}
}
