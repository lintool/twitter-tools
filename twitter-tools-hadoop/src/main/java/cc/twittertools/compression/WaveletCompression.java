package cc.twittertools.compression;

import java.nio.ByteOrder;
import java.nio.IntBuffer;

import org.jcodings.util.ArrayCopy;

import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;

public class WaveletCompression {
	// FastPFor compression initialization
	private static FastPFOR p4 = new FastPFOR();
	// block number of FastPFor compression
	private static int blocks = RawCountCompression.NUM_INTERVALS / RawCountCompression.BLOCK_SIZE; 
	// left number compressed by Variable Byte encoding.
	private static int left = RawCountCompression.NUM_INTERVALS % RawCountCompression.BLOCK_SIZE;
	
	public static byte[] PForDeltaCompression(int[] data) {
		int[] blockData = new int[blocks*RawCountCompression.BLOCK_SIZE];
		System.arraycopy(data, 0, blockData, 0, blockData.length);
		int[] blockDWTData = WaveletTransformation.encode(blockData);
		
		// store sign bits separately
		byte[] blockSignBits = new byte[blockDWTData.length/8];
		for(int i=0; i<blockDWTData.length; i++) {
			blockSignBits[i/8] <<= 1;
			// 1 represents non-negative, 0 represents negative
			blockSignBits[i/8] |= (blockDWTData[i]>=0 ? 1: 0); 
			blockDWTData[i] = blockDWTData[i] * (blockDWTData[i]>=0 ? 1: -1);
		}
		
		// using PForDelta algorithm to compress unsigned blockDWTData
		int[] out = new int[blocks * RawCountCompression.BLOCK_SIZE];
		
		IntWrapper inPos = new IntWrapper(0);
		IntWrapper outPos = new IntWrapper(0);
		p4.compress(blockDWTData, inPos, blocks * RawCountCompression.BLOCK_SIZE, out, outPos);
		int[] outData = new int[outPos.get()];
		System.arraycopy(out, 0, outData, 0, outData.length);
		// convert int array to byte array;
		byte[] blockCompression = ArrayCopy.int2byte(outData);
		
		int[] leftData = new int[left];
		System.arraycopy(data, blocks * RawCountCompression.BLOCK_SIZE, leftData, 0, left);
		byte[] leftCompression = VariableByteCode.encode(leftData);
		
		//combine
		byte[] compression = new byte[blockCompression.length+leftCompression.length+blockSignBits.length+2];
		// The first two bytes store the boundary of block part and left part
		compression[0] = (byte) (blockCompression.length >> 8);
		compression[1] = (byte) (blockCompression.length);
		System.arraycopy(blockCompression, 0, compression, 2,
				blockCompression.length);
		System.arraycopy(blockSignBits, 0, compression, blockCompression.length+2, blockSignBits.length);
		System.arraycopy(leftCompression, 0, compression,
				blockCompression.length + blockSignBits.length + 2, leftCompression.length);

		return compression;
	}
	
	public static int[] PForDeltaDecompression(byte[] compressData) {
		int boundary = (int) compressData[0] & 0xff;
		boundary <<= 8;
		boundary |= (int) compressData[1] & 0xff;
		byte[] blockCompression = new byte[boundary];
		byte[] blockSignedBits = new byte[blocks*RawCountCompression.BLOCK_SIZE/8];
		byte[] leftCompression = new byte[compressData.length - boundary - blockSignedBits.length - 2];
		System.arraycopy(compressData, 2, blockCompression, 0, boundary);
		System.arraycopy(compressData, boundary+2, blockSignedBits, 0, blockSignedBits.length);
		System.arraycopy(compressData, boundary + blockSignedBits.length + 2, leftCompression, 0,
				leftCompression.length);
		
		IntBuffer intBuffer = java.nio.ByteBuffer.wrap(blockCompression)
				.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
		int[] rawInts = new int[intBuffer.remaining()];
		intBuffer.get(rawInts);

		int[] blockData = new int[blocks*RawCountCompression.BLOCK_SIZE];
		IntWrapper inPos = new IntWrapper(0);
		IntWrapper outPos = new IntWrapper(0);
		p4.uncompress(rawInts, inPos, rawInts.length, blockData, outPos);
		
		byte[] options = {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, (byte) 0x80};
		for(int i=0; i<blocks*RawCountCompression.BLOCK_SIZE; i++) {
			if( (blockSignedBits[i/8] & options[7-i%8]) == 0 ){ //negative bit
				blockData[i] *= -1;
			}
		}
		
		int[] decodeData = WaveletTransformation.decode(blockData);
		int[] originalData = new int[RawCountCompression.NUM_INTERVALS];
		System.arraycopy(decodeData, 0, originalData, 0, decodeData.length);
		
		int count = 0;
		for (Integer i : VariableByteCode.decode(leftCompression)) {
			originalData[RawCountCompression.BLOCK_SIZE * blocks + count++] = i;
		}
		
		return originalData;
	}
}
