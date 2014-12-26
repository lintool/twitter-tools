package cc.twittertools.compression;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.BiMap;

import cc.twittertools.encoding.HuffmanEncoding;
import cc.twittertools.encoding.HuffmanTree.Unit;
import cc.twittertools.encoding.Simple16Encoding;
import cc.twittertools.encoding.VariableByteEncoding;

public class CompressionEnsemble {
	
	public static final int P4D = 0;
	public static final int VB = 1;
	public static final int S16 = 2;
	public static final int DWT_P4D = 3;
	public static final int DWT_VB = 4;
	public static final int HUFFMAN = 5;
	public static final int HUFFMAN_VB = 6;
	
	public static enum ALG_ENUM {P4D, VB, S16, DWT_P4D, DWT_VB, HUFFMAN, HUFFMAN_VB};
	public static Map<Integer, Algorithm> algorithmMap;
	public static CompressionEnsemble ensemble = new CompressionEnsemble();
	public static BiMap<Unit, String> huffmanTree;
	public static int counter = 0;
	
	public class Algorithm {
		int algorithm;
		long bytes;
		long decodeTime;
		
		public Algorithm(int algorithm) {
			this.algorithm = algorithm;
			this.bytes = 0;
			this.decodeTime = 0;
		}
		
		public void increment(long bytes, long decodeTime) {
			this.bytes += bytes;
			this.decodeTime += decodeTime;
		}
	}
	
	public static void init(BiMap<Unit, String> inputHuffmanTree) {
		huffmanTree = inputHuffmanTree;
		algorithmMap = new HashMap<Integer, Algorithm>();
		for (ALG_ENUM algo: ALG_ENUM.values()) {
			algorithmMap.put(algo.ordinal(), ensemble.new Algorithm(algo.ordinal()));
		}
	}
	
	public static void compression(int[] counts) throws IOException{
		counter++;
		byte[] compression = null;
		int[] decompression = null;
		int[] encodeData = null, originalData = null;
		long startTime = 0, endTime = 0;
		for (ALG_ENUM algo: ALG_ENUM.values()) {
			switch (algo.ordinal()){
				case 0: // PForDelta
					compression = RawCountCompression.PForDeltaCompression(counts);
					startTime = System.currentTimeMillis();
					decompression = RawCountCompression.PForDeltaDecompression(compression);
					endTime = System.currentTimeMillis();
					break;
				case 1: // Variable Byte
					compression = VariableByteEncoding.encode(counts);
					startTime = System.currentTimeMillis();
					decompression = VariableByteEncoding.decode(compression);
					endTime = System.currentTimeMillis();
					break;
				case 2: // Simple 16
					compression = Simple16Encoding.encode(counts);
					startTime = System.currentTimeMillis();
					decompression = Simple16Encoding.decode(compression);
					endTime = System.currentTimeMillis();
					break;
				case 3: // PForDelta after Wavelet Transformation
					compression = WaveletCompression.PForDeltaCompression(counts);
					startTime = System.currentTimeMillis();
					decompression = WaveletCompression.PForDeltaDecompression(compression);
					endTime = System.currentTimeMillis();
					break;
				case 4: // Variable Bytes after Wavelet Transformation
					compression = WaveletCompression.VariableByteCompression(counts);
					startTime = System.currentTimeMillis();
					decompression = WaveletCompression.VariableByteDecompression(compression);
					endTime = System.currentTimeMillis();
					break;
				case 5: 
					encodeData = HuffmanEncoding.encode(counts, huffmanTree);
					startTime = System.currentTimeMillis();
					decompression = HuffmanEncoding.decode(encodeData, huffmanTree);
					endTime = System.currentTimeMillis();
					break;
				case 6:
					encodeData = HuffmanEncoding.encode(counts, huffmanTree);
					compression = VariableByteEncoding.encode(encodeData);
					startTime = System.currentTimeMillis();
					decompression = VariableByteEncoding.decode(compression);
					originalData = HuffmanEncoding.decode(decompression, huffmanTree);
					endTime = System.currentTimeMillis();
					break;
			}
			
			int algoId = algo.ordinal();
			int bytes = (algoId != 5 ? compression.length : encodeData.length * 4);
			Algorithm algorithm = algorithmMap.get(algoId);
			algorithm.increment(bytes, endTime - startTime);
			algorithmMap.put(algoId, algorithm);
		}
	}
	
	public static void printResults() {
		System.out.println("Total " + counter + " Words");
		for (ALG_ENUM algo: ALG_ENUM.values()) {
			Algorithm algorithm = algorithmMap.get(algo.ordinal());
			int KB = 1024;
			System.out.print(algo.name() + " bytes: " + algorithm.bytes / KB);
			System.out.println(" decode time: " + algorithm.decodeTime / 1000);
		}
	}
}
