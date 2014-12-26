package cc.twittertools.encoding;

import gnu.trove.TIntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.tools.ant.taskdefs.Length;

public class Simple16Encoding{
	
	
	public static int byteArrayToInt(byte[] encodedValue) {
	    int index = 0;
	    int value = encodedValue[index++] << Byte.SIZE * 3;
	    value ^= (encodedValue[index++] & 0xFF) << Byte.SIZE * 2;
	    value ^= (encodedValue[index++] & 0xFF) << Byte.SIZE * 1;
	    value ^= (encodedValue[index++] & 0xFF);
	    return value;
	}

	public static byte[] intToByteArray(int value) {
	    int index = 0;
	    byte[] encodedValue = new byte[Integer.SIZE / Byte.SIZE];
	    encodedValue[index++] = (byte) (value >> Byte.SIZE * 3);
	    encodedValue[index++] = (byte) (value >> Byte.SIZE * 2);   
	    encodedValue[index++] = (byte) (value >> Byte.SIZE);   
	    encodedValue[index++] = (byte) value;
	    return encodedValue;
	}
	
  /* (non-Javadoc)
   * @see org.sindice.index.benchmark.scalability.codec.Codec#decode(byte[])
   */
  public static int[] decode(final byte[] codes) throws IOException {
    final TIntArrayList values = new TIntArrayList();
    final ByteArrayInputStream input = new ByteArrayInputStream(codes);
    final byte[] reusableByteArray = new byte[4];
    final byte[] length = new byte[2];
    input.read(length);
    int size = (int) length[0] & 0xff;
    size <<= 8;
    size |= (int) length[1] & 0xff;
    
    int word = 0;
    while (input.available() > 0) {
      input.read(reusableByteArray);
      word = byteArrayToInt(reusableByteArray);
      values.add(decode(word));
    }
    if(values.size() > size){
    	values.remove(size, values.size() - size);
    }
    return values.toNativeArray();
  }

  /* (non-Javadoc)
   * @see org.sindice.index.benchmark.scalability.codec.Codec#encode(java.util.List)
   */
  public static byte[] encode(final int[] values) throws IOException{
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] length = new byte[2];
    length[0] = (byte) (values.length >> 8);
    length[1] = (byte) values.length;
    out.write(length);
    
    int i = 0;

    // While we didn't encode all the values, continue
    while (i < values.length) {
      int word = 0;

      // Start iteration over selectors
      for (int k = 0; k < 16; k++) {
        int j = 0, ncodes = 0;
        // Encode selector
        word = k << 28;
        // How many codes we can fit ?
        ncodes = (cnum[k] < values.length - i) ? cnum[k] : values.length - i;

        // Start iteration over values
        // if a value is > to the allowed code size, break loop and try next selector
        for (int shift = 0; (j < ncodes) && values[i +j] < (1 << cbits[k][j]); j++) {
          word += values[i +j] << shift;
          // Increment shift with code width
          shift += cbits[k][j];
        }

        // if all codes have been fitted, convert the word and return a byte array
        if (j == ncodes) {
          i = i + ncodes;
          // Append word
          out.write(intToByteArray(word));
          // Break the for loop
          break;
        }
      }
    }

    return out.toByteArray();
  }

  protected static int[] decode(final int word) {
    int[] values;
    int i = 0;
    // Unsigned right shift
    final int selector = word >>> 28;

    switch(selector) {
      case 0:
        values = new int[28];

        values[i] = word & 1; i++;
        values[i] = (word>>1) & 1; i++;
        values[i] = (word>>2) & 1; i++;
        values[i] = (word>>3) & 1; i++;
        values[i] = (word>>4) & 1; i++;
        values[i] = (word>>5) & 1; i++;
        values[i] = (word>>6) & 1; i++;
        values[i] = (word>>7) & 1; i++;
        values[i] = (word>>8) & 1; i++;
        values[i] = (word>>9) & 1; i++;
        values[i] = (word>>10) & 1; i++;
        values[i] = (word>>11) & 1; i++;
        values[i] = (word>>12) & 1; i++;
        values[i] = (word>>13) & 1; i++;
        values[i] = (word>>14) & 1; i++;
        values[i] = (word>>15) & 1; i++;
        values[i] = (word>>16) & 1; i++;
        values[i] = (word>>17) & 1; i++;
        values[i] = (word>>18) & 1; i++;
        values[i] = (word>>19) & 1; i++;
        values[i] = (word>>20) & 1; i++;
        values[i] = (word>>21) & 1; i++;
        values[i] = (word>>22) & 1; i++;
        values[i] = (word>>23) & 1; i++;
        values[i] = (word>>24) & 1; i++;
        values[i] = (word>>25) & 1; i++;
        values[i] = (word>>26) & 1; i++;
        values[i] = (word>>27) & 1; i++;
        return values;

      case 1:
        values = new int[21];

        values[i] = (word) & 3;    i++;
        values[i] = (word>>2) & 3; i++;
        values[i] = (word>>4) & 3; i++;
        values[i] = (word>>6) & 3; i++;
        values[i] = (word>>8) & 3; i++;
        values[i] = (word>>10) & 3; i++;
        values[i] = (word>>12) & 3; i++;
        values[i] = (word>>14) & 1; i++;
        values[i] = (word>>15) & 1; i++;
        values[i] = (word>>16) & 1; i++;
        values[i] = (word>>17) & 1; i++;
        values[i] = (word>>18) & 1; i++;
        values[i] = (word>>19) & 1; i++;
        values[i] = (word>>20) & 1; i++;
        values[i] = (word>>21) & 1; i++;
        values[i] = (word>>22) & 1; i++;
        values[i] = (word>>23) & 1; i++;
        values[i] = (word>>24) & 1; i++;
        values[i] = (word>>25) & 1; i++;
        values[i] = (word>>26) & 1; i++;
        values[i] = (word>>27) & 1; i++;
        return values;

      case 2:
        values = new int[21];

        values[i] = (word) & 1; i++;
        values[i] = (word>>1) & 1; i++;
        values[i] = (word>>2) & 1; i++;
        values[i] = (word>>3) & 1; i++;
        values[i] = (word>>4) & 1; i++;
        values[i] = (word>>5) & 1; i++;
        values[i] = (word>>6) & 1; i++;
        values[i] = (word>>7) & 3; i++;
        values[i] = (word>>9) & 3; i++;
        values[i] = (word>>11) & 3; i++;
        values[i] = (word>>13) & 3; i++;
        values[i] = (word>>15) & 3; i++;
        values[i] = (word>>17) & 3; i++;
        values[i] = (word>>19) & 3; i++;
        values[i] = (word>>21) & 1; i++;
        values[i] = (word>>22) & 1; i++;
        values[i] = (word>>23) & 1; i++;
        values[i] = (word>>24) & 1; i++;
        values[i] = (word>>25) & 1; i++;
        values[i] = (word>>26) & 1; i++;
        values[i] = (word>>27) & 1; i++;
        return values;

      case 3:
        values = new int[21];

        values[i] = (word) & 1; i++;
        values[i] = (word>>1) & 1; i++;
        values[i] = (word>>2) & 1; i++;
        values[i] = (word>>3) & 1; i++;
        values[i] = (word>>4) & 1; i++;
        values[i] = (word>>5) & 1; i++;
        values[i] = (word>>6) & 1; i++;
        values[i] = (word>>7) & 1; i++;
        values[i] = (word>>8) & 1; i++;
        values[i] = (word>>9) & 1; i++;
        values[i] = (word>>10) & 1; i++;
        values[i] = (word>>11) & 1; i++;
        values[i] = (word>>12) & 1; i++;
        values[i] = (word>>13) & 1; i++;
        values[i] = (word>>14) & 3; i++;
        values[i] = (word>>16) & 3; i++;
        values[i] = (word>>18) & 3; i++;
        values[i] = (word>>20) & 3; i++;
        values[i] = (word>>22) & 3; i++;
        values[i] = (word>>24) & 3; i++;
        values[i] = (word>>26) & 3; i++;
        return values;

      case 4:
        values = new int[14];

        values[i] = (word) & 3; i++;
        values[i] = (word>>2) & 3; i++;
        values[i] = (word>>4) & 3; i++;
        values[i] = (word>>6) & 3; i++;
        values[i] = (word>>8) & 3; i++;
        values[i] = (word>>10) & 3; i++;
        values[i] = (word>>12) & 3; i++;
        values[i] = (word>>14) & 3; i++;
        values[i] = (word>>16) & 3; i++;
        values[i] = (word>>18) & 3; i++;
        values[i] = (word>>20) & 3; i++;
        values[i] = (word>>22) & 3; i++;
        values[i] = (word>>24) & 3; i++;
        values[i] = (word>>26) & 3; i++;
        return values;

        case 5:
          values = new int[9];

        values[i] = (word) & 15; i++;
        values[i] = (word>>4) & 7; i++;
        values[i] = (word>>7) & 7; i++;
        values[i] = (word>>10) & 7; i++;
        values[i] = (word>>13) & 7; i++;
        values[i] = (word>>16) & 7; i++;
        values[i] = (word>>19) & 7; i++;
        values[i] = (word>>22) & 7; i++;
        values[i] = (word>>25) & 7; i++;
        return values;

        case 6:
          values = new int[8];

          values[i] = (word) & 7; i++;
          values[i] = (word>>3) & 15; i++;
          values[i] = (word>>7) & 15; i++;
          values[i] = (word>>11) & 15; i++;
          values[i] = (word>>15) & 15; i++;
          values[i] = (word>>19) & 7; i++;
          values[i] = (word>>22) & 7; i++;
          values[i] = (word>>25) & 7; i++;
          return values;

        case 7:
          values = new int[7];

          values[i] = (word) & 15; i++;
          values[i] = (word>>4) & 15; i++;
          values[i] = (word>>8) & 15; i++;
          values[i] = (word>>12) & 15; i++;
          values[i] = (word>>16) & 15; i++;
          values[i] = (word>>20) & 15; i++;
          values[i] = (word>>24) & 15; i++;
          return values;

        case 8:
          values = new int[6];

          values[i] = (word) & 31; i++;
          values[i] = (word>>5) & 31; i++;
          values[i] = (word>>10) & 31; i++;
          values[i] = (word>>15) & 31; i++;
          values[i] = (word>>20) & 15; i++;
          values[i] = (word>>24) & 15; i++;
          return values;

        case 9:
          values = new int[6];

          values[i] = (word) & 15; i++;
          values[i] = (word>>4) & 15; i++;
          values[i] = (word>>8) & 31; i++;
          values[i] = (word>>13) & 31; i++;
          values[i] = (word>>18) & 31; i++;
          values[i] = (word>>23) & 31; i++;
          return values;

        case 10:
          values = new int[5];

          values[i] = (word) & 63; i++;
          values[i] = (word>>6) & 63; i++;
          values[i] = (word>>12) & 63; i++;
          values[i] = (word>>18) & 31; i++;
          values[i] = (word>>23) & 31; i++;
          return values;

        case 11:
          values = new int[5];

          values[i] = (word) & 31; i++;
          values[i] = (word>>5) & 31; i++;
          values[i] = (word>>10) & 63; i++;
          values[i] = (word>>16) & 63; i++;
          values[i] = (word>>22) & 63; i++;
          return values;

        case 12:
          values = new int[4];

          values[i] = (word) & 127; i++;
          values[i] = (word>>7) & 127; i++;
          values[i] = (word>>14) & 127; i++;
          values[i] = (word>>21) & 127; i++;
          return values;

        case 13:
          values = new int[3];

          values[i] = (word) & 1023; i++;
          values[i] = (word>>10) & 511; i++;
          values[i] = (word>>19) & 511; i++;
          return values;

        case 14:
          values = new int[2];

          values[i] = (word) & 16383; i++;
          values[i] = (word>>14) & 16383; i++;
          return values;

        case 15:
          values = new int[1];

          values[i] = (word) & ((1<<28)-1); i++;
          return values;

        default:
          throw new RuntimeException("Unknown selector: " + selector);
      }

  }

  protected static int cbits[][] = { {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
                                     {2,2,2,2,2,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0},
                                     {1,1,1,1,1,1,1,2,2,2,2,2,2,2,1,1,1,1,1,1,1,0,0,0,0,0,0,0},
                                     {1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,0,0,0,0,0,0,0},
                                     {2,2,2,2,2,2,2,2,2,2,2,2,2,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
                                     {4,3,3,3,3,3,3,3,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
                                     {3,4,4,4,4,3,3,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
                                     {4,4,4,4,4,4,4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
                                     {5,5,5,5,4,4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
                                     {4,4,5,5,5,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
                                     {6,6,6,5,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
                                     {5,5,6,6,6,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
                                     {7,7,7,7,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
                                     {10,9,9,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
                                     {14,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
                                     {28,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0} };

    protected static int cnum[] = {28, 21, 21, 21, 14, 9, 8, 7, 6, 6, 5, 5, 4, 3, 2, 1};

}