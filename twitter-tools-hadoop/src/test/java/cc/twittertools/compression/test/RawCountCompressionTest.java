package cc.twittertools.compression.test;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import cc.twittertools.compression.*;

public class RawCountCompressionTest {

	private static int[] count = new int[RawCountCompression.NUM_INTERVALS];
	private static Random rand = new Random();

	@Before
	public void setUp() {
		int max = 65536, min = 0;
		for (int i = 0; i < count.length; i++) {
			count[i] = rand.nextInt(max - min) + min;
		}
	}

	@Test
	public void testPForDelta() {
		byte[] compressData = RawCountCompression.PForDeltaCompression(count);
		int[] decompressData = RawCountCompression
				.PForDeltaDecompression(compressData);
		assertArrayEquals(decompressData, count);
	}

	@Test
	public void testVariableByte() {
		byte[] compressData = RawCountCompression
				.variableByteCompression(count);
		int[] decompressData = RawCountCompression
				.variableByteDecompression(compressData);
		assertArrayEquals(decompressData, count);
	}

	@Test
	public void testWaveletTransformation() {
		// generate random data
		int[] testData = new int[256];
		int max = 65536, min = 0;
		for (int i = 0; i < testData.length; i++) {
			testData[i] = rand.nextInt(max - min) + min;
		}

		// encoding and decoding
		int[] encodeData = WaveletTransformation.encode(testData);
		int[] decodeData = WaveletTransformation.decode(encodeData);

		int errorRange = (int) (Math.log(256) / Math.log(2));
		assertEquals(testData.length, decodeData.length);
		for (int i = 0; i < decodeData.length; i++) {
			assertTrue(testData[i] > decodeData[i] - errorRange);
			assertTrue(testData[i] < decodeData[i] + errorRange);
		}
	}
	
	@Test
	public void testDWTPForDelta(){
		byte[] compressData = WaveletCompression.PForDeltaCompression(count);
		int[] decompressData = WaveletCompression.PForDeltaDecompression(compressData);
		int errorRange = (int) (Math.log(256) / Math.log(2));
		for(int i=0; i<decompressData.length;i++){
			if(i<256){
				assertTrue(count[i] > decompressData[i] - errorRange);
				assertTrue(count[i] < decompressData[i] + errorRange);
			}else{
				assertTrue(count[i] == decompressData[i]);
			}
		}
	}
}
