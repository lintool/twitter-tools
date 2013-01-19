/*
 *    Copyright (c) Sematext International
 *    All Rights Reserved
 *
 *    THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF Sematext International
 *    The copyright notice above does not evidence any
 *    actual or intended publication of such source code.
 */

package cc.twittertools.download;

import java.io.File;
import java.util.ArrayList;

public class DirectoryManager {
  private String absolutePath;
  private String outputPath;
  private int docCount = 0;
  private int currentDocIndex = 0;
  private ArrayList docList;

  public DirectoryManager(String absolutePath, String outputPath) {
    this.absolutePath = absolutePath;
    this.outputPath = outputPath;
    this.docList = new ArrayList<String>();
  }

  /**
   * Get the list of files present at the root level
   */
  public void createDocumentList() {
    File curDir = new File(this.absolutePath);
    String[] curFiles = curDir.list();
    this.elaborateFolder(curFiles, this.absolutePath, this.outputPath);
    System.out.println("Document list loaded: " + this.docList.size() + " elements.");
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
      
      if(curPath.compareTo(this.absolutePath) == 0 && !curFile.isDirectory()) continue;
      
      //if (curFile.isDirectory() && curFile.getName().startsWith("2011")) {
      if (curFile.isDirectory()) {
        String[] folderFiles = curFile.list();
        
        String CurFolder = curOutPath + files[i] + "/"; 
        File OD = new File(CurFolder); 
        boolean A = OD.mkdir();
        
        this.elaborateFolder(folderFiles, curPath + files[i] + "/", curOutPath + files[i] + "/");
      } else if(curPath.compareTo(this.absolutePath) != 0) {
        this.docList.add(this.docList.size(), curFile.getAbsolutePath());
      }
    }
  }

  
  /**
   * Get the next document to elaborate
   */
  public synchronized String getNextDoc() {
    if(this.currentDocIndex == this.docList.size()) return null;
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
  public ArrayList<String> getDocList() {
    return this.docList;
  }
}
