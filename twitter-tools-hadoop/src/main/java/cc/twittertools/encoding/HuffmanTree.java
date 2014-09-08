package cc.twittertools.encoding;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.pig.builtin.REPLACE;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.sun.org.apache.xalan.internal.xsltc.compiler.Pattern;
import com.sun.xml.bind.annotation.OverrideAnnotationOf;


/** This class implements a binary tree suitable for use in Huffman coding.
  * @author Michael Barnathan
*/
public class HuffmanTree implements Comparable<HuffmanTree> {
	
	public class Unit{
		int data[];
		
		public Unit(String str){
			String[] data = str.replace("[", "").replace("]", "").split(", ");
			this.data = new int[data.length];
			for(int i=0; i<data.length; i++){
				this.data[i] = Integer.parseInt(data[i]);
			}
		}
		
		public Unit(int[] data){
			this.data = data;
		}
		
		public Unit(Unit u){
			if(u == null) return;
			this.data = new int[u.data.length];
			for(int i=0; i<u.data.length; i++){
				this.data[i] = u.data[i];
			}
		}
		
		@Override
		public boolean equals(Object obj){
			if (!(obj instanceof Unit))
	            return false;
	        if (obj == this)
	            return true;
	        
	        Unit u = (Unit) obj;
	        if(u.data.length != this.data.length){
	        	return false;
	        }
	        
	        for(int i=0; i<u.data.length; i++){
	        	if(this.data[i] != u.data[i]){
	        		return false;
	        	}
	        }
	        return true;
		}
		
		@Override
		public int hashCode(){
			String s = Arrays.toString(data);
			return s.hashCode();
		}
		
		@Override
		public String toString(){
			return Arrays.toString(data);
		}
	}
	/** The value stored in this tree node. */
	public Unit value;

	/** The left child of the tree. */
	public HuffmanTree left;

	/** The right child of the tree. */
	public HuffmanTree right;

	/** The codeword frequency of the value. */
	public int frequency;

	/** Creates a new Huffman tree with the specified value, frequency, and children. */
	public HuffmanTree(Unit value, int freq, HuffmanTree left, HuffmanTree right) {
		this.value = new Unit(value);
		this.frequency = freq;
		this.left = left;
		this.right = right;
	}

	/** Creates a leaf node with the specified value and frequency. */
	public HuffmanTree(Unit value, int freq) { this(value, freq, null, null); }

	/** Creates a null-valued 0-frequency leaf node. */
	public HuffmanTree() { this(null, 0); }


	/** Builds a HuffmanTree from a FrequencyTable.
	  * @param freqs a table containing character frequencies.
	  * @return a Huffman tree containing the characters' prefix codes. */
	public static HuffmanTree buildTree(HashMap<Unit, Integer> freqs) {
		//The Huffman algorithm operates by growing a tree from the
		//two lowest frequencies specified in the table.
		//These naturally form a heap, implemented as a priority queue.

		PriorityQueue<HuffmanTree> huffq = new PriorityQueue<HuffmanTree>();

		//First, add the leaves of the tree directly from the frequency table.
		for (Map.Entry<Unit, Integer> mapping : freqs.entrySet())
			huffq.add(new HuffmanTree(mapping.getKey(), mapping.getValue()));

		//Next, iteratively build the tree by merging queue nodes until only 1 remains.
		while (huffq.size() > 1) {
			HuffmanTree newnode = new HuffmanTree();
			newnode.left = huffq.remove();
			newnode.right = huffq.remove();

			//The frequencies are added. The value remains null.
			newnode.frequency = newnode.left.frequency + newnode.right.frequency;

			//Re-enqueue the merged node (2 nodes to 1; the queue size shrinks).
			huffq.add(newnode);
		}

		//The node remaining in the queue becomes the root of the Huffman tree.
		return huffq.remove();
	}


	/** This function traverses the tree and returns all
	  * of the Huffman codes contained within it.
	  * @return a HashMap containing mappings between characters and codes, in ASCII. */
	public final BiMap<Unit, String> getCodes() {
		//No use traversing an empty tree!
		if (left == null && right == null)
			return null;

		BiMap<Unit, String> ret = HashBiMap.create();

		//Traverse the tree and populate the HashMap each time we reach a leaf.
		traverse("", ret);
		return ret;
	}


	/** A recursive tree traversal call. Builds Huffman codes when leaves are traversed. */
	private void traverse(String currentcode, BiMap<Unit, String> curmappings) {
		//Associate each leaf's value with its Huffman code.
		if (left == null && right == null)
			curmappings.put(value, currentcode);
		
		if (left != null)
			left.traverse(currentcode + "1", curmappings);

		if (right != null)
			right.traverse(currentcode + "0", curmappings);
	}

	/** Compares by frequency. */
	public int compareTo(HuffmanTree rhs) {
		return (frequency < rhs.frequency) ? -1 : ((frequency > rhs.frequency) ? 1 : 0);
	}
	
	public static void main(String[] args){
		HuffmanTree s = new HuffmanTree();
		HashMap<Unit, Integer> freqTable = new HashMap<Unit, Integer>();
		int[] a = {12, 0, 1};
		int[] b = {1, 1, 0};
		int[] c = {0, 1, 0};
		int[] d = {0, 0, 0};
		freqTable.put(s.new Unit(a),1);
		freqTable.put(s.new Unit(b),4);
		freqTable.put(s.new Unit(c),5);
		freqTable.put(s.new Unit(d),6);
		HuffmanTree huffmanTree = HuffmanTree.buildTree(freqTable);
		BiMap<Unit, String> codes = huffmanTree.getCodes();
		for(Map.Entry<Unit, String> e: codes.entrySet()){
			System.out.println(e.getKey()+":"+e.getValue());
		}
		BiMap<String, Unit> codeReverse = codes.inverse();
		for(Map.Entry<String, Unit> e: codeReverse.entrySet()){
			System.out.println(e.getKey()+":"+e.getValue());
		}
		System.out.println(codeReverse.get("110").toString());
	}
}