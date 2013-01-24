package cc.twittertools.download;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class DirectoryManager {
  private static final Logger LOG = Logger.getLogger(DirectoryManager.class);
  
  private final String absolutePath;
  private final String outputPath;
  private int docCount = 0;
  private int currentDocIndex = 0;
  private ArrayList docList;

  public DirectoryManager(String absolutePath, String outputPath) {
    this.absolutePath = Preconditions.checkNotNull(absolutePath);
    this.outputPath = Preconditions.checkNotNull(outputPath);
    this.docList = Lists.newArrayList();
  }

  /**
   * Get the list of files present at the root level
   */
  public void createDocumentList() {
    File curDir = new File(this.absolutePath);
    String[] curFiles = curDir.list();
    this.elaborateFolder(curFiles, this.absolutePath, this.outputPath);
    LOG.info("Document list loaded: " + this.docList.size() + " elements.");
  }

  /**
   * Support to the elaborateDocuments function to loop over all files
   */
  private void elaborateFolder(String[] files, String curPath, String curOutPath) {
    for (int i = 0; i < files.length; i++) {
      if (files[i].indexOf(".svn") == 0) {
        continue;
      }
      File curFile = new File(curPath + files[i]);
      
      if (curPath.compareTo(this.absolutePath) == 0 && !curFile.isDirectory()) {
        continue;
      }
      
      if (curFile.isDirectory()) {
        String[] folderFiles = curFile.list();
        
        String curFolder = curOutPath + files[i] + "/"; 
        File oD = new File(curFolder); 
        boolean a = oD.mkdir();
        
        this.elaborateFolder(folderFiles, curPath + files[i] + "/", curOutPath + files[i] + "/");
      } else if (curPath.compareTo(this.absolutePath) != 0) {
        this.docList.add(this.docList.size(), curFile.getAbsolutePath());
      }
    }
  }

  
  /**
   * Get the next document to elaborate
   */
  public synchronized String getNextDoc() {
    if (this.currentDocIndex == this.docList.size()) {
      return null;
    }
    
    String nextDocName = (String) this.docList.get(new Integer(this.currentDocIndex));
    
    if (nextDocName == null) {
      return null;
    }
    
    this.currentDocIndex++;
    return nextDocName;
  }

  /**
   * Debug methods to explore the produced HashMaps
   */
  public List<String> getDocList() {
    return this.docList;
  }
}
