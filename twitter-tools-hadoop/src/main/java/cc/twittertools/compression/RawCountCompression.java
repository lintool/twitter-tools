package cc.twittertools.compression;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jcodings.util.ArrayCopy;

import cc.twittertools.hbase.LoadWordCount;
import cc.twittertools.hbase.WordCountDAO;
import cc.twittertools.hbase.WordCountDAO.WordCount;

import com.google.common.collect.Table;
import com.google.common.primitives.Ints;

import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;

public class RawCountCompression {

	public static int NUM_INTERVALS = 288;
	public static int BLOCK_SIZE = 128;
	// FastPFor compression initialization
	private static FastPFOR p4 = new FastPFOR();
	// block number of FastPFor compression
	private static int blocks = NUM_INTERVALS / BLOCK_SIZE; 
	// left number compressed by Variable Byte encoding.
	private static int left = NUM_INTERVALS % BLOCK_SIZE;

	public static byte[] PForDeltaCompression(int[] data) {
		// add integer compression here
		// the first blocks*128 integers are compressed using PForDelta
		// encoding,
		// while the left integers using VB encode.
		int[] out = new int[blocks * BLOCK_SIZE];

		IntWrapper inPos = new IntWrapper(0);
		IntWrapper outPos = new IntWrapper(0);
		p4.compress(data, inPos, blocks * BLOCK_SIZE, out, outPos);
		int[] blockData = new int[outPos.get()];
		System.arraycopy(out, 0, blockData, 0, outPos.get());
		// convert int array to byte array;
		byte[] blockCompression = ArrayCopy.int2byte(blockData);

		int[] leftData = new int[left];
		System.arraycopy(data, blocks * BLOCK_SIZE, leftData, 0, left);
		byte[] leftCompression = VariableByteCode.encode(leftData);

		// combine
		byte[] compression = new byte[blockCompression.length + leftCompression.length + 2];
		// The first two bytes store the boundary of block part and left part
		compression[0] = (byte) (blockCompression.length >> 8);
		compression[1] = (byte) (blockCompression.length);
		System.arraycopy(blockCompression, 0, compression, 2,
				blockCompression.length);
		System.arraycopy(leftCompression, 0, compression,
				blockCompression.length + 2, leftCompression.length);

		return compression;
	}

	public static int[] PForDeltaDecompression(byte[] compressData) {
		int boundary = (int) compressData[0] & 0xff;
		boundary <<= 8;
		boundary |= (int) compressData[1] & 0xff;
		byte[] compressPart1 = new byte[boundary];
		byte[] compressPart2 = new byte[compressData.length - boundary - 2];
		System.arraycopy(compressData, 2, compressPart1, 0, boundary);
		System.arraycopy(compressData, boundary + 2, compressPart2, 0,
				compressPart2.length);

		IntBuffer intBuffer = java.nio.ByteBuffer.wrap(compressPart1)
				.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
		int[] rawInts = new int[intBuffer.remaining()];
		intBuffer.get(rawInts);

		int[] originalData = new int[NUM_INTERVALS];
		IntWrapper inPos = new IntWrapper(0);
		IntWrapper outPos = new IntWrapper(0);
		p4.uncompress(rawInts, inPos, rawInts.length, originalData, outPos);

		int count = 0;
		for (Integer i : VariableByteCode.decode(compressPart2)) {
			originalData[BLOCK_SIZE * blocks + count++] = i;
		}
		return originalData;
	}

	public static byte[] variableByteCompression(int[] data) {
		return VariableByteCode.encode(data);
	}

	public static int[] variableByteDecompression(byte[] compressData) {
		List<Integer> data = VariableByteCode.decode(compressData);
		return Ints.toArray(data);
	}

	public static byte[] integerNoCompression(int[] data) {
		return ArrayCopy.int2byte(data);
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("invalid argument");
		}

		long PForBytes = 0;
		long VBBytes = 0;
		long integerBytes = 0;
		Table<String, String, WordCountDAO.WordCount> wordCountMap = LoadWordCount
				.LoadWordCountMap(args[0]);
		for (WordCountDAO.WordCount w : wordCountMap.values()) {
			PForBytes += PForDeltaCompression(w.count).length;
			VBBytes += variableByteCompression(w.count).length;
			integerBytes += integerNoCompression(w.count).length;
		}
		System.out.println("Bytes After PForDelta Compression: " + PForBytes);
		System.out.println("Bytes After VariableBytes Compression: " + VBBytes);
		System.out.println("Integer Bytes With No Compression: " + integerBytes);
	}
}
