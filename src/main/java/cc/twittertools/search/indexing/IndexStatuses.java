package cc.twittertools.search.indexing;


import java.io.File;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;


import cc.twittertools.corpus.data.JsonStatusCorpusReader;
import cc.twittertools.corpus.data.Status;
import cc.twittertools.corpus.data.StatusStream;
import cc.twittertools.corpus.data.TSVStatusCorpusReader;
import cc.twittertools.search.configuration.IndriIndexingParams;



/**
 * Reference implementation for indexing statuses.
 */
public class IndexStatuses {


	public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_41);
	public static String corpusFormat = null;
	
	private IndexStatuses() {}

	public static enum StatusField {
		ID("id"),
		SCREEN_NAME("screen_name"),
		CREATED_AT("create_at"),
		TEXT("text"),
		DAY("day");

		public final String name;

		StatusField(String s) {
			name = s;
		}
	};


	  
  enum CorpusFormat {
    TSV("tsv"),
    JSON("json"),
    TRECTEXT("trecText");
    
    public final String name;

    CorpusFormat(String s) {
      name = s;
    }
  };


	public static void main(String[] args) throws Exception {


	    
		String pathToCorpusDir = null;
		String pathToIndex = null;

		
		
		
		try {
		  IndriIndexingParams params = new IndriIndexingParams();
      params.ParseXMLQueryFile(args[0]);
      
      pathToIndex = params.getIndexName();
      System.out.println("Building Index: " + pathToIndex);
      if(pathToIndex == null) {
        System.err.println("IndexEnvironment: null value for index name!");
        throw new IllegalArgumentException();     
      }
      pathToCorpusDir = params.getPathToCorpus();
      System.out.println("Corpus: " + pathToCorpusDir);
      if(pathToCorpusDir == null) {
        System.err.println("IndexEnvironment: null value for corpus path!");
        throw new IllegalArgumentException();     
      }
      corpusFormat = params.getFormat();
      System.out.println("Corpus format: " + corpusFormat);
      if(corpusFormat == null) {
        System.err.println("IndexEnvironment: null value for corpus path!");
        throw new IllegalArgumentException();     
      }
      boolean knownFormat = false;
      for(CorpusFormat format : CorpusFormat.values()) {
        if(corpusFormat.equalsIgnoreCase(format.name)) {
          knownFormat = true;
          break;
        }
      }
      if(! knownFormat ) {
        System.err.println("IndexEnvironment: no recognized corpus format supplied: <tsv, json, trecText>.");
        throw new IllegalArgumentException(); 
      }
		} catch (Exception e) {
		  IndexStatuses.help();
		  System.exit(-1);
		}
		


		StatusStream stream;


    
		File file = new File(pathToCorpusDir);
		if (!file.exists()) {
			System.err.println("Error: " + file + " does not exist!");
			System.exit(-1);
		}


		
		if(corpusFormat.equalsIgnoreCase("tsv")) {
		  stream = new TSVStatusCorpusReader(file);
		} else {
	    stream = new JsonStatusCorpusReader(file);
		}

		

		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_41);

		Directory dir = FSDirectory.open(new File(pathToIndex));
		IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_41, analyzer);
		iwc.setOpenMode(OpenMode.CREATE);
		
		IndexWriter writer = new IndexWriter(dir, iwc);

		int cnt = 0;
		Status status;
		try {
			while ((status = stream.next()) != null) {
				if (status.getText() == null) {
					continue;
				}

				cnt++;
				Document doc = new Document();
				doc.add(new LongField("id", status.getId(), Field.Store.YES));
				doc.add(new TextField("screen_name", status.getScreenname(), Store.YES));
				doc.add(new TextField("created_at",  status.getCreatedAt(), Store.YES));
				doc.add(new TextField("text",        status.getText(), Store.YES));

				
				writer.addDocument(doc);
				if (cnt % 10000 == 0) {
					System.out.println(cnt + " statuses indexed");
				}
			}



			writer.close();
		} finally {
			stream.close();
		}

		System.out.println(String.format("Total of %s statuses indexed", cnt));
	}

	
	public static void help() {
	  System.err.println("expected arguments: /path/to/config/file");
	  System.err.println();
	  System.err.println("where config file is of the structure:");
	  System.err.println();
	  System.err.println("<parameters>");
    System.err.println("<index>/path/to/index/to/build</index>");
    System.err.println("<corpus>/path/to/corpus/to/index</corpus>");
    System.err.println("<corpusFormat>[tsv, json, trecText]</corpusFormat>");
    System.err.println("</parameters>");
    System.err.println();
    System.err.println("It is assumed that the corpus path points to a directory full of gzipped files.");
    System.err.println("The corpusFormat element specifies how the files in the corpus are formatted.");



	}


}
