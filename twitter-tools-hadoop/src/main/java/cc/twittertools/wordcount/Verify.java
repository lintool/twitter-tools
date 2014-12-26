package cc.twittertools.wordcount;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import cc.twittertools.encoding.HuffmanEncoding;
import cc.twittertools.encoding.VariableByteEncoding;
import cc.twittertools.encoding.HuffmanTree.Unit;

import com.google.common.collect.BiMap;
import com.google.common.collect.Table.Cell;

public class Verify {
	
	public static BiMap<Unit, String> huffmanTree;
	
	public static void verify(MemoryCounts M) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter("output/sample-verify-bigram.counts"));
		for (Cell<String, Integer, Integer> cell : M.termIdTable.cellSet()) {
			String word = cell.getRowKey();
			int dayDiff = cell.getColumnKey();
			int termId = cell.getValue();
			int offset = M.offset.get(termId);
			int length = M.length.get(termId);
			byte[] compressData = M.data.subList(offset, offset+length).toByteArray();
			int[] decompressData = VariableByteEncoding.decode(compressData);
			int[] origData = HuffmanEncoding.decode(decompressData, huffmanTree);
			for (int i = 0; i < origData.length; i++) {
				int count = origData[i];
				if (count != 0) {
					bw.write(word + " " + dayDiff + " " + termId + " " + i +" " + count + "\n");
				}
			}
		}
		bw.close();
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		String filePath = "output/sample-bigram.counts";
		String huffmanTreePath = "output/sample-huffmantree.txt";
		MemoryCounts M = MemoryCounts.load(filePath);
		huffmanTree = HuffmanEncoding.loadHuffmanTree(huffmanTreePath);
		verify(M);
	}

}
