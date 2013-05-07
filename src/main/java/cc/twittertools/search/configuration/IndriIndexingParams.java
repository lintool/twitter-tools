package cc.twittertools.search.configuration;

 import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Utility class for parsing a file full of indri queries stored as XML.
 * Not really for human consumption.  Use IndriQueries class for direct interaction.
 * 
 * @author Miles Efron
 *
 */
public class IndriIndexingParams extends DefaultHandler {
  

  
	private String indexName;
	private String pathToCorpus;
	private List<String> fields;
	private String format;       // <tsv, json, trecText>
	private String tempVal = "";

	public void ParseXMLQueryFile(String fileName)  {
		SAXParserFactory spf = SAXParserFactory.newInstance();
		try {
			SAXParser sp = spf.newSAXParser();
			sp.parse(fileName, this);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	public void startElement(String uri, String localName, String qName, Attributes atts) {
		tempVal = "";
		if(qName.equalsIgnoreCase("parameters")) {
			fields = new LinkedList<String>();
		}
	}
	public void characters(char[] ch, int start, int length) throws SAXException {
		tempVal = new String(ch,start,length);
	}
	public void endElement(String uri, String localName,
			String qName) throws SAXException {

		if(qName.equalsIgnoreCase("index")) {
			indexName = tempVal;
		} else if (qName.equalsIgnoreCase("corpus")) {
			pathToCorpus = tempVal;
		} else if (qName.equalsIgnoreCase("field")) {
			fields.add(tempVal);
		} else if (qName.equalsIgnoreCase("corpusFormat")) {
      format = tempVal;
    } else {
			//System.err.println("Bad param: " + qName + ":: " + tempVal);
			//System.exit(-1);
		} 

	}


	public String getIndexName() {
		return indexName;
	}
	public String getPathToCorpus() {
		return pathToCorpus;
	}
	public List<String> getFields() {
		return fields;
	}
	public String getFormat() {
	  return format;
	}

}
