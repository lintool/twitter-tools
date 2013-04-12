package cc.twittertools.search.configuration;



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
public class IndriRunParams extends DefaultHandler {
	private String indexName;
	private int count = -1;
	private int fbDocs = -1;
	private int fbTerms = -1;
	private double fbOrigWeight = -1.0;
	private String tempVal;
	private String similarity;

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
	}
	public void characters(char[] ch, int start, int length) throws SAXException {
		tempVal = new String(ch,start,length);
	}
	public void endElement(String uri, String localName,
			String qName) throws SAXException {

		if(qName.equalsIgnoreCase("index")) {
			indexName = tempVal;
		} else if (qName.equalsIgnoreCase("count")) {
			try {
				count = Integer.parseInt(tempVal);
			} catch (Exception e) {
				e.printStackTrace();
			}			
		} else if (qName.equalsIgnoreCase("fbDocs")) {
			try {
				fbDocs = Integer.parseInt(tempVal);
			} catch (Exception e) {
				e.printStackTrace();
			}	
		} else if (qName.equalsIgnoreCase("fbTerms")) {
			try {
				fbTerms = Integer.parseInt(tempVal);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else if (qName.equalsIgnoreCase("similarity")) {
      try {
        similarity = tempVal;
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else if (qName.equalsIgnoreCase("fbOrigWeight")) {
			try {
				fbOrigWeight = Double.parseDouble(tempVal);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}


	public String getIndexName() {
		return indexName;
	}
	public int getCount() {
		return count;
	}
	public int getFBDocs() {
		return fbDocs;
	}
	public int getFBTerms() {
		return fbTerms;
	}
	public double fbOrigWeight() {
		return fbOrigWeight();
	}
	public String getSimilarity() {
	  return similarity;
	}

}
