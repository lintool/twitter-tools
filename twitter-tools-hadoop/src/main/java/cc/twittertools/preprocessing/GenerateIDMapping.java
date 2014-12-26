package cc.twittertools.preprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class GenerateIDMapping {
	private static final Set<String> idSet = new TreeSet<String>();
	
	public static HashMap<String, Integer> loadIDMapping(String filePath) throws IOException{
		HashMap<String, Integer> idMap = new HashMap<String, Integer>();
		BufferedReader bf = new BufferedReader(new FileReader(filePath));
		String line;
		while((line=bf.readLine())!=null){
			String[] groups = line.split("\\s+");
			idMap.put(groups[0], Integer.parseInt(groups[1]));
		}
		return idMap;
	}
	
	public static void readRecordFromFile(String filePath) throws IOException{
		System.out.println("Processing "+filePath);
		BufferedReader bf = new BufferedReader(new FileReader(filePath));
		String line;
		while((line=bf.readLine())!=null){
			String[] groups = line.split("\\t");
			if(groups.length != 5) 
				continue;
			String userId = groups[3];
			idSet.add(userId);
		}
	}
	
	public static void readRecordFromDir(String path) throws IOException{
		File folder = new File(path);
		if(folder.isDirectory()){
			for (File file : folder.listFiles()) {
				if(!file.getName().startsWith("part"))
					continue;
				String filePath = path+file.getName();
				readRecordFromFile(filePath);
			}
		}
	}
	
	public static void main(String[] args) throws IOException{
		if(args.length!=1){
			System.out.println("invalid argument");
		}
		readRecordFromDir(args[0]);
		BufferedWriter bw = new BufferedWriter(new FileWriter("id-mapping.txt"));
		int id = 0;
		for(String s: idSet){
			bw.write(s+" "+id+"\n");
			id++;
		}
		bw.close();
	}
	
}
