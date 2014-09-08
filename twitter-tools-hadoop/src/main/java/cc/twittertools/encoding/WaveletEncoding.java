package cc.twittertools.encoding;

import org.junit.Assert;


public class WaveletEncoding {

	private static boolean isPowerOfTwo(int x) {
		while (((x % 2) == 0) && x > 1) { /* While x is even and > 1 */
			x /= 2;
		}
		return x == 1;
	}

	public static int[] encode(int[] input) {
		Assert.assertTrue(isPowerOfTwo(input.length));
		
		if (input.length == 1) return input;
		
		int[] delta = new int[input.length];
		int[] average = new int[input.length];
		int iterations = input.length;
		int level = 0;
		
		while (iterations > 1) {
			int beginPos, endPos;
			if (level == 0) {
				beginPos = 0;
				endPos = input.length;
			} else {
				beginPos = input.length / (int) Math.pow(2, level);
				endPos = beginPos * 2;
			}
			int basisPos = iterations / 2;
			for (int i = beginPos; i < endPos && basisPos > 1; i = i + 2) {
				delta[basisPos + (i - beginPos) / 2] = input[i] - input[i + 1];
				average[basisPos + (i - beginPos) / 2] = (int) ((input[i] + input[i + 1]) / 2);
			}
			if (basisPos == 1 && input.length >= 4) {
				delta[0] = (int) ((input[2] + input[3]) / 2);
				delta[1] = input[2] - input[3];
			} else if (basisPos == 1 && input.length == 2) {
				delta[0] = (int) ((input[0] + input[1]) / 2);
				delta[1] = input[0] - input[1];
			}
			input = average;
			iterations /= 2;
			level++;
		}
		
		return delta;
	}

	public static int[] decode(int[] delta) {
		Assert.assertTrue(isPowerOfTwo(delta.length));
		
		int[] original = new int[delta.length];
		int iterations = delta.length;
		int level = 1;
		
		while (iterations > 1) {
			int interval = (int) Math.pow(2, level - 1);
			for (int i = 0; i < interval; i++) {
				original[2 * i] = delta[i] + delta[i + interval]/2;
				original[2 * i + 1] = delta[i] - delta[i + interval]/2;
			}
			System.arraycopy(original, 0, delta, 0, 2 * interval);
			iterations /= 2;
			level++;
		}
		return delta;
	}
}
