package cc.twittertools.search.configuration;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import cc.twittertools.search.Queries;

/**
 * Utility class for parsing a file full of indri queries stored as XML.
 * Not really for human consumption.  Use IndriQueries class for direct interaction.
 * 
 * @author Miles Efron
 *
 */
public class IndriQueryParams extends DefaultHandler {
	private Queries queries;
	private String tempVal = "";
	
	public void ParseXMLQueryFile(String fileName) throws Exception{
		SAXParserFactory spf = SAXParserFactory.newInstance();
		SAXParser sp = spf.newSAXParser();
		sp.parse(fileName, this);
	}
	
	public void startElement(String uri, String localName, String qName, Attributes atts) {
		tempVal = "";
		if(qName.equalsIgnoreCase("parameters")) {
			queries = new Queries();
		}
	}
	public void characters(char[] ch, int start, int length) throws SAXException {
		tempVal = new String(ch,start,length);
	}
	public void endElement(String uri, String localName,
			String qName) throws SAXException {

			if(qName.equalsIgnoreCase("number")) {
				queries.addQueryID(tempVal);
			} else if (qName.equalsIgnoreCase("text")) {
				queries.addQueryString(tempVal);
			} else if (qName.equalsIgnoreCase("firstrel")) {
				queries.addFirstRel(tempVal);
			} else if (qName.equalsIgnoreCase("lastrel")) {
				queries.addLastRel(tempVal);
			} 

		}
	
	
	public Queries getQueries() {
		return queries;
	}

}
