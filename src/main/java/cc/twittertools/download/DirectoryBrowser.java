package cc.twittertools.download;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.naming.OperationNotSupportedException;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class DirectoryBrowser implements Iterable<String> {
  private static final Logger LOG = Logger.getLogger(DirectoryBrowser.class);
  
  private final String absolutePath;
  private final String outputPath;
  private int docCount = 0;
  private int currentDocIndex = 0;
  private ArrayList docList;

  public DirectoryBrowser(String absolutePath, String outputPath) {
    this.absolutePath = Preconditions.checkNotNull(absolutePath);
    this.outputPath = Preconditions.checkNotNull(outputPath);
    this.docList = Lists.newArrayList();
  }

  /**
   * Get the list of files present at the root level
   */
  private void createDocumentList() {
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
  private synchronized String getNextDoc() {
    if (this.currentDocIndex == this.docList.size()) {
      throw new NoSuchElementException();
    }
    
    String nextDocName = (String) this.docList.get(new Integer(this.currentDocIndex));
    
    if (nextDocName == null) {
      return null;
    }
    
    this.currentDocIndex++;
    return nextDocName;
  }

  private boolean hasNext() {
    return this.currentDocIndex < this.docList.size();
  }

  public Iterator<String> iterator() {
    return new DirectoryIterator(this);
  }

  private class DirectoryIterator implements Iterator<String> {
    DirectoryBrowser directoryManager;
    public DirectoryIterator(DirectoryBrowser directoryManager) {
      this.directoryManager = directoryManager;
      this.directoryManager.createDocumentList();
    }

    public boolean hasNext() {
      return directoryManager.hasNext();
    }

    public String next() {
      return directoryManager.getNextDoc();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
