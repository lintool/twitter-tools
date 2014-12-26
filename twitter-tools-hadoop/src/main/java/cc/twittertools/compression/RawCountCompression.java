package cc.twittertools.compression;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.File;

import org.jcodings.util.ArrayCopy;

import cc.twittertools.encoding.Simple16Encoding;
import cc.twittertools.encoding.VariableByteEncoding;
import cc.twittertools.hbase.WordCountDAO;
import cc.twittertools.hbase.WordCountDAO.WordCount;
import cc.twittertools.wordcount.UnigramComparison;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.primitives.Ints;

import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;

public class RawCountCompression {

	public static int BLOCK_SIZE = 128;
	// FastPFor compression initialization
	private static FastPFOR p4 = new FastPFOR();

	public static byte[] PForDeltaCompression(int[] data) throws IOException {
		// add integer compression here
		// the first blocks*128 integers are compressed using PForDelta
		// encoding, while the left integers using VB encode.
		int blocks = data.length / BLOCK_SIZE;
		int left = data.length % BLOCK_SIZE;
		int[] out = new int[blocks * BLOCK_SIZE];
		
		if (blocks == 0) {
			return Simple16Encoding.encode(data);
		}

		IntWrapper inPos = new IntWrapper(0);
		IntWrapper outPos = new IntWrapper(0);
		p4.compress(data, inPos, blocks * BLOCK_SIZE, out, outPos);
		int[] blockData = new int[outPos.get()];
		System.arraycopy(out, 0, blockData, 0, outPos.get());
		// convert int array to byte array;
		byte[] blockCompression = ArrayCopy.int2byte(blockData);

		int[] leftData = new int[left];
		System.arraycopy(data, blocks * BLOCK_SIZE, leftData, 0, left);
		byte[] leftCompression = Simple16Encoding.encode(leftData);

		// combine
		byte[] compression = new byte[blockCompression.length + leftCompression.length + 4];
		compression[0] = (byte) (data.length >> 8);
		compression[1] = (byte) (data.length);
		// The second two bytes store the boundary of block part and left part
		compression[2] = (byte) (blockCompression.length >> 8);
		compression[3] = (byte) (blockCompression.length);
		System.arraycopy(blockCompression, 0, compression, 4,
				blockCompression.length);
		System.arraycopy(leftCompression, 0, compression,
				blockCompression.length + 4, leftCompression.length);

		return compression;
	}

	public static int[] PForDeltaDecompression(byte[] compressData) throws IOException {
		int origLength = (int) compressData[0] & 0xff;
		origLength <<= 8;
		origLength |= (int) compressData[1] & 0xff;
		if (origLength < BLOCK_SIZE){
			return Simple16Encoding.decode(compressData);
		}
		
		int boundary = (int) compressData[2] & 0xff;
		boundary <<= 8;
		boundary |= (int) compressData[3] & 0xff;
		
		byte[] compressPart1 = new byte[boundary];
		byte[] compressPart2 = new byte[compressData.length - boundary - 4];
		System.arraycopy(compressData, 4, compressPart1, 0, boundary);
		System.arraycopy(compressData, boundary + 4, compressPart2, 0,
				compressPart2.length);

		IntBuffer intBuffer = java.nio.ByteBuffer.wrap(compressPart1)
				.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
		int[] rawInts = new int[intBuffer.remaining()];
		intBuffer.get(rawInts);
		
		int blocks = origLength / BLOCK_SIZE;
		int left = origLength % BLOCK_SIZE;
		int[] originalData = new int[origLength];
		IntWrapper inPos = new IntWrapper(0);
		IntWrapper outPos = new IntWrapper(0);
		p4.uncompress(rawInts, inPos, rawInts.length, originalData, outPos);

		int count = 0;
		int[] leftData = Simple16Encoding.decode(compressPart2);
		System.arraycopy(leftData, 0, originalData, blocks*BLOCK_SIZE, leftData.length);
		return originalData;
	}

	public static byte[] integerNoCompression(int[] data) {
		return ArrayCopy.int2byte(data);
	}
	
	private static int average(int[] data) {
		int sum = 0;
		for(int i: data) {
			sum += i;
		}
		return sum/data.length;
	}
}
