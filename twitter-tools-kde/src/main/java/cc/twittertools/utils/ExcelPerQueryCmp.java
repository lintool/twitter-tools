package cc.twittertools.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class ExcelPerQueryCmp {
	
	public static void main(String[] args) throws IOException {
		String inputFileAddr = "../twitter-tools-rm3/output/rm3.perquery.2011.test.txt";
		String outputFileAddr = inputFileAddr.replace(".txt", ".csv");
		BufferedReader br = new BufferedReader(new FileReader(inputFileAddr));
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileAddr));
		bw.write("topic,map,P_30\n");
		String line;
		String map = null, P_30;
		while((line = br.readLine()) != null) {
			String[] arr = line.split("\\s+");
			if (arr[0].equals("map")) {
				map = arr[2];
			} else if (arr[0].equals("P_30")) {
				P_30 = arr[2];
				bw.write(arr[1]+","+map+","+P_30+"\n");
			}
		}
		bw.close();
	}
}
