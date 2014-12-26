package cc.twittertools.compression;

import java.io.IOException;
import java.util.List;

import javax.xml.crypto.Data;

import org.jcodings.util.ArrayCopy;

import com.google.common.primitives.Ints;

import me.lemire.integercompression.FastPFOR;

public class HuffmanCompression {
	public static int BLOCK_SIZE = 128;
	// FastPFor compression initialization
	private static FastPFOR p4 = new FastPFOR();
	
	public static byte[] PForDeltaCompression(int[] data) throws IOException {
		if(data.length < BLOCK_SIZE) {
			return RawCountCompression.integerNoCompression(data);
		}
		return RawCountCompression.PForDeltaCompression(data);
	}
	
	public static int[] PForDeltaDecompression(byte[] compressData) throws IOException {
		return RawCountCompression.PForDeltaDecompression(compressData);
	}
	
	public static byte[] integerNoCompression(int[] data) {
		return ArrayCopy.int2byte(data);
	}
}
