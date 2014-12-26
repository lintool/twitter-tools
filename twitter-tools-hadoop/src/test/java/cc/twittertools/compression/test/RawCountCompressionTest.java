package cc.twittertools.compression.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.BiMap;

import cc.twittertools.compression.*;
import cc.twittertools.encoding.HuffmanEncoding;
import cc.twittertools.encoding.HuffmanTree;
import cc.twittertools.encoding.Simple16Encoding;
import cc.twittertools.encoding.HuffmanTree.Unit;
import cc.twittertools.encoding.VariableByteEncoding;
import cc.twittertools.encoding.WaveletEncoding;
import cc.twittertools.hbase.WordCountDAO;

public class RawCountCompressionTest {
	
	private static Random rand = new Random();
	private static int[] count = new int[288];
	@Before
	public void setUp() {
		int max = 10, min = 0;
		for (int i = 0; i < count.length; i++) {
			count[i] = rand.nextInt(max - min) + min;
		}
	}

	@Test
	public void testPForDelta() throws IOException {
		long beforeCompress = System.currentTimeMillis();
		byte[] compressData = RawCountCompression.PForDeltaCompression(count);
		System.out.println("P4D: "+compressData.length);
		long endCompress = System.currentTimeMillis();
		int[] decompressData = RawCountCompression
				.PForDeltaDecompression(compressData);
		long endDecompress = System.currentTimeMillis();
		
		assertArrayEquals(decompressData, count);
		long endJudge = System.currentTimeMillis();
		System.out.println("compress:"+(endCompress-beforeCompress)+
				"decompress:"+(endDecompress-endCompress)
				+"judge:" + (endJudge-endDecompress));
	}

	@Test
	public void testVariableByte() {
		byte[] compressData = VariableByteEncoding.encode(count);
		System.out.println("VB: "+compressData.length);
		int[] decompressData = VariableByteEncoding.decode(compressData);
		assertArrayEquals(decompressData, count);
	}
	
	@Test
	public void testSimple16() throws IOException {
		byte[] compressData = Simple16Encoding.encode(count);
		System.out.println("S16: "+compressData.length);
		int[] decompressData = Simple16Encoding.decode(compressData);
		assertArrayEquals(decompressData, count);
	}
	
	@Test
	public void testWaveletEncoding() {
		// generate random data
		int[] testData = new int[256];
		int max = 65536, min = 0;
		for (int i = 0; i < testData.length; i++) {
			testData[i] = rand.nextInt(max - min) + min;
		}

		// encoding and decoding
		int[] encodeData = WaveletEncoding.encode(testData);
		int[] decodeData = WaveletEncoding.decode(encodeData);

		int errorRange = (int) (Math.log(256) / Math.log(2));
		assertEquals(testData.length, decodeData.length);
		for (int i = 0; i < decodeData.length; i++) {
			assertTrue(testData[i] > decodeData[i] - errorRange);
			assertTrue(testData[i] < decodeData[i] + errorRange);
		}
	}
	
	@Test
	public void testDWTPForDelta() throws IOException{
		byte[] compressData = WaveletCompression.PForDeltaCompression(count);
		System.out.println("Wavelet P4D: "+compressData.length);
		int[] decompressData = WaveletCompression.PForDeltaDecompression(compressData);
		int lastPowNum = getLastPowerNumber(count.length);
		int errorRange = (int) (Math.log(lastPowNum) / Math.log(2));
		for(int i=0; i<decompressData.length;i++){
			if(i<lastPowNum){
				assertTrue(count[i] >= decompressData[i] - errorRange);
				assertTrue(count[i] <= decompressData[i] + errorRange);
			}else{
				assertTrue(count[i] == decompressData[i]);
			}
		}
	}
	
	@Test
	public void testDWTVariableByte(){
		byte[] compressData = WaveletCompression.VariableByteCompression(count);
		System.out.println("Wavelet VB: "+compressData.length);
		int[] decompressData = WaveletCompression.VariableByteDecompression(compressData);
		int lastPowNum = getLastPowerNumber(count.length);
		int errorRange = (int) (Math.log(lastPowNum) / Math.log(2));
		for(int i=0; i<decompressData.length;i++){
			if(i<lastPowNum){
				assertTrue(count[i] >= decompressData[i] - errorRange);
				assertTrue(count[i] <= decompressData[i] + errorRange);
			}else{
				assertTrue(count[i] == decompressData[i]);
			}
		}
	}
	
	@Test
	public void testHuffmanEncoding() throws IOException{
		WordCountDAO.WordCount w = new WordCountDAO.WordCount("at", "201312", count);
		HashMap<Unit, Integer> freqs = new HashMap<Unit, Integer>();
		HuffmanEncoding.GenerateFreqDict(w.count, freqs);
		HuffmanTree tree = HuffmanTree.buildTree(freqs);
		BiMap<Unit, String> huffmanCodes = tree.getCodes();
		int[] codes = HuffmanEncoding.encode(w.count, huffmanCodes);
		byte[] compression = VariableByteEncoding.encode(codes);
		int [] decompression = VariableByteEncoding.decode(compression);
		int[] decodes = HuffmanEncoding.decode(decompression, huffmanCodes);
		assertArrayEquals(decodes, count);
	}
	
	public int getLastPowerNumber(int n){
		int num = 1;
		while(n > 1){
			num *= 2;
			n /= 2;
		}
		return num;
	}
}
