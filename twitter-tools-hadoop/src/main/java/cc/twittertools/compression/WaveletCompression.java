package cc.twittertools.compression;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.List;

import org.eclipse.jdt.core.dom.ThisExpression;
import org.jcodings.util.ArrayCopy;

import cc.twittertools.encoding.Simple16Encoding;
import cc.twittertools.encoding.VariableByteEncoding;
import cc.twittertools.encoding.WaveletEncoding;

import com.google.common.primitives.Ints;

import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;

public class WaveletCompression {
	public static int BLOCK_SIZE = 128;
	// FastPFor compression initialization
	private static FastPFOR p4 = new FastPFOR();
	
	public static byte[] PForDeltaCompression(int[] data) throws IOException {
		int blocks = data.length / BLOCK_SIZE;
		int left = data.length % BLOCK_SIZE;
		
		if (blocks == 0) {
			return Simple16Encoding.encode(data);
		}
		
		IntArrayList dataList = IntArrayList.wrap(data);
		int[] blockData = new int[blocks * BLOCK_SIZE];
		System.arraycopy(data, 0, blockData, 0, blockData.length);
		int[] blockDWTData = WaveletEncoding.encode(blockData);

		// store sign bits separately
		byte[] blockSignBits = new byte[blockDWTData.length / 8];
		for (int i = 0; i < blockDWTData.length; i++) {
			blockSignBits[i / 8] <<= 1;
			// 1 represents non-negative, 0 represents negative
			blockSignBits[i / 8] |= (blockDWTData[i] >= 0 ? 1 : 0);
			blockDWTData[i] = blockDWTData[i] * (blockDWTData[i] >= 0 ? 1 : -1);
		}

		// using PForDelta algorithm to compress unsigned blockDWTData
		int[] out = new int[blocks * BLOCK_SIZE];

		IntWrapper inPos = new IntWrapper(0);
		IntWrapper outPos = new IntWrapper(0);
		p4.compress(blockDWTData, inPos, blocks * BLOCK_SIZE, out, outPos);
		int[] outData = new int[outPos.get()];
		System.arraycopy(out, 0, outData, 0, outData.length);
		// convert int array to byte array;
		byte[] blockCompression = ArrayCopy.int2byte(outData);

		// using Variable Byte algorithm to compress left numbers
		int[] leftData = new int[left];
		System.arraycopy(data, blocks * RawCountCompression.BLOCK_SIZE,
				leftData, 0, left);
		byte[] leftCompression = Simple16Encoding.encode(leftData);

		// combine
		byte[] compression = new byte[blockCompression.length
				+ leftCompression.length + blockSignBits.length + 4];
		compression[0] = (byte) (data.length >> 8);
		compression[1] = (byte) (data.length);
		// The first two bytes store the boundary of block part and left part
		compression[2] = (byte) (blockCompression.length >> 8);
		compression[3] = (byte) (blockCompression.length);
		System.arraycopy(blockCompression, 0, compression, 4,
				blockCompression.length);
		System.arraycopy(blockSignBits, 0, compression,
				blockCompression.length + 4, blockSignBits.length);
		System.arraycopy(leftCompression, 0, compression,
				blockCompression.length + blockSignBits.length + 4,
				leftCompression.length);

		return compression;
	}

	public static int[] PForDeltaDecompression(byte[] compressData) throws IOException {
		int origLength = (int) compressData[0] & 0xff;
		origLength <<= 8;
		origLength |= (int) compressData[1] & 0xff;
		int blocks = origLength / BLOCK_SIZE;
		if (blocks == 0) {
			return Simple16Encoding.decode(compressData);
		}
		
		int boundary = (int) compressData[2] & 0xff;
		boundary <<= 8;
		boundary |= (int) compressData[3] & 0xff;
		int left = origLength % BLOCK_SIZE;
		
		// compressData is composed of three parts: block data, sign bits of
		// block data, left data
		byte[] blockCompression = new byte[boundary];
		byte[] blockSignedBits = new byte[blocks * BLOCK_SIZE / 8];
		byte[] leftCompression = new byte[compressData.length - boundary
				- blockSignedBits.length - 4];
		System.arraycopy(compressData, 4, blockCompression, 0, boundary);
		System.arraycopy(compressData, boundary + 4, blockSignedBits, 0,
				blockSignedBits.length);
		System.arraycopy(compressData, boundary + blockSignedBits.length + 4,
				leftCompression, 0, leftCompression.length);

		// uncompress block data using PForDelta algorithm
		IntBuffer intBuffer = java.nio.ByteBuffer.wrap(blockCompression)
				.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
		int[] rawInts = new int[intBuffer.remaining()];
		intBuffer.get(rawInts);

		int blockLength = blocks * BLOCK_SIZE;
		int[] blockData = new int[blockLength];
		IntWrapper inPos = new IntWrapper(0);
		IntWrapper outPos = new IntWrapper(0);
		p4.uncompress(rawInts, inPos, rawInts.length, blockData, outPos);

		// recover sign bits to block data
		byte[] options = { 0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40,
				(byte) 0x80 };
		for (int i = 0; i < blockLength; i++) {
			int maxBits = (i >= blockLength - blockLength % 8)? blockLength % 8 -1 : 7; 
			if ((blockSignedBits[i / 8] & options[maxBits - i % 8]) == 0) { // negative bit
				blockData[i] *= -1;
			}
		}

		// decoding using wavelet transformation
		int[] decodeData = WaveletEncoding.decode(blockData);
		int[] leftData = Simple16Encoding.decode(leftCompression);
		int[] originalData = new int[origLength];
		System.arraycopy(decodeData, 0, originalData, 0, decodeData.length);
		System.arraycopy(leftData, 0, originalData, blocks*BLOCK_SIZE, leftData.length);

		return originalData;
	}

	public static byte[] VariableByteCompression(int[] data) {
		//int blocks = data.length / BLOCK_SIZE;
		//int left = data.length % BLOCK_SIZE;
		int blockLength = getLargestPowerNumber(data.length);
		int left = data.length - blockLength;
		int[] blockData = new int[blockLength];
		System.arraycopy(data, 0, blockData, 0, blockData.length);
		int[] blockDWTData = WaveletEncoding.encode(blockData);

		// store sign bits separately
		byte[] blockSignBits = new byte[(int) Math.ceil(blockDWTData.length/8.0)];
		for (int i = 0; i < blockDWTData.length; i++) {
			blockSignBits[i / 8] <<= 1;
			// 1 represents non-negative, 0 represents negative
			blockSignBits[i / 8] |= (blockDWTData[i] >= 0 ? 1 : 0);
			blockDWTData[i] = blockDWTData[i] * (blockDWTData[i] >= 0 ? 1 : -1);
		}

		// using Variable Byte algorithm to compress left numbers
		int[] leftData = new int[left];
		System.arraycopy(data, blockLength, leftData, 0, left);

		byte[] blockCompression = VariableByteEncoding.encode(blockDWTData);
		byte[] leftCompression = VariableByteEncoding.encode(leftData);

		byte[] compression = new byte[blockCompression.length
				+ leftCompression.length + blockSignBits.length + 4];
		compression[0] = (byte) (data.length >> 8);
		compression[1] = (byte) (data.length);
		// The first two bytes store the boundary of block part and left part
		compression[2] = (byte) (blockCompression.length >> 8);
		compression[3] = (byte) (blockCompression.length);
		System.arraycopy(blockCompression, 0, compression, 4,
				blockCompression.length);
		System.arraycopy(blockSignBits, 0, compression,
				blockCompression.length + 4, blockSignBits.length);
		System.arraycopy(leftCompression, 0, compression,
				blockCompression.length + blockSignBits.length + 4,
				leftCompression.length);

		return compression;
	}

	public static int[] VariableByteDecompression(byte[] compressData) {
		int origLength = (int) compressData[0] & 0xff;
		origLength <<= 8;
		origLength |= (int) compressData[1] & 0xff;
		int boundary = (int) compressData[2] & 0xff;
		boundary <<= 8;
		boundary |= (int) compressData[3] & 0xff;
		int blockLength = getLargestPowerNumber(origLength);
		int left = origLength - blockLength;
		// compressData is composed of three parts: block data, sign bits of
		// block data, left data
		byte[] blockCompression = new byte[boundary];
		byte[] blockSignedBits = new byte[(int) Math.ceil(blockLength/8.0)];
		byte[] leftCompression = new byte[compressData.length - boundary
				- blockSignedBits.length - 4];
		System.arraycopy(compressData, 4, blockCompression, 0, boundary);
		System.arraycopy(compressData, boundary + 4, blockSignedBits, 0,
				blockSignedBits.length);
		System.arraycopy(compressData, boundary + blockSignedBits.length + 4,
				leftCompression, 0, leftCompression.length);

		int[] blockData = VariableByteEncoding.decode(blockCompression);
		int[] leftData = VariableByteEncoding.decode(leftCompression);
		byte[] options = { 0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, (byte)0x80 };
		for (int i = 0; i < blockLength; i++) {
			int maxBits = (i >= blockLength - blockLength % 8)? blockLength % 8 -1 : 7; 
			if ((blockSignedBits[i / 8] & options[maxBits - i % 8]) == 0) { // negative bit
				blockData[i] *= -1;
			}
		}

		int[] decodeData = WaveletEncoding.decode(blockData);

		int[] originalData = new int[origLength];
		System.arraycopy(decodeData, 0, originalData, 0, decodeData.length);
		System.arraycopy(leftData, 0, originalData, decodeData.length,
				leftData.length);

		return originalData;
	}
	
	// return the largest number in [0, limit] which is power of 2
	public static int getLargestPowerNumber(int limit){
		int num = 1;
		while(limit > 1){
			num *= 2;
			limit /= 2;
		}
		return num;
	}
}
