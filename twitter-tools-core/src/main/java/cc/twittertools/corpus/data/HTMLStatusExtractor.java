package cc.twittertools.corpus.data;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class HTMLStatusExtractor {

    public SimpleDateFormat date_fmt = new SimpleDateFormat("EEE MMM d kk:mm:ss Z yyyy");

    public HTMLStatusExtractor() {
	date_fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static Map<String, String> splitQuery(URL url) 
	throws java.io.UnsupportedEncodingException {
	Map<String, String> query_pairs = new LinkedHashMap<String, String>();
	String query = url.getQuery();
	String[] pairs = query.split("&");
	for (String pair : pairs) {
	    int idx = pair.indexOf("=");
	    query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"),
			    URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
	}
	return query_pairs;
    }

    public JsonObject extractTweet(String html) 
	throws java.net.MalformedURLException, java.io.UnsupportedEncodingException {
	JsonObject status = new JsonObject();

	Document doc = Jsoup.parse(html);
	Element tweet_div = doc.select("div.permalink-tweet").first();

	String tweet_text = tweet_div.select("p.tweet-text").first().text();
	status.addProperty("text", tweet_text);

	String tweet_id = tweet_div.attr("data-tweet-id");
	status.addProperty("id_str", tweet_id);
	status.addProperty("id", Long.parseLong(tweet_id));

	String timestamp = doc.select("span.js-short-timestamp").first().attr("data-time");
	Date created_at = new Date();
	created_at.setTime(Long.parseLong(timestamp) * 1000);
	status.addProperty("created_at", date_fmt.format(created_at));

	Elements js_stats_retweets = doc.select("li.js-stat-retweets");
	if (!js_stats_retweets.isEmpty()) {
	    status.addProperty("retweeted", true);
	    String count = js_stats_retweets.select("strong").first().text();
	    status.addProperty("retweet_count", Long.parseLong(count));
	} else {
	    status.addProperty("retweeted", false);
	    status.addProperty("retweet_count", 0);
	}
	Elements js_stats_favs = doc.select("li.js-stat-favorites");
	status.addProperty("favorited", !js_stats_favs.isEmpty());
	    

	// User subfield
	JsonObject user = new JsonObject();
	String user_id = tweet_div.attr("data-user-id");
	user.addProperty("id_str", user_id);
	user.addProperty("id", Long.parseLong(user_id));
	String screen_name = tweet_div.attr("data-screen-name");
	user.addProperty("screen_name", screen_name);
	String user_name = tweet_div.attr("data-name");
	user.addProperty("name", user_name);
	
	status.add("user", user);
	
	// Geo information
	Elements tweet_loc = doc.select("a.tweet-geo-text");
	if (!tweet_loc.isEmpty()) {
	    JsonObject location = new JsonObject();
	    Element loc = tweet_loc.first();
	    // Adding http to avoid malformed URL exception
	    URL url = new URL("http:" + loc.attr("href"));
	    Map<String, String> query_params = HTMLStatusExtractor.splitQuery(url);
	    // Loop over possible query parameters
	    // http://asnsblues.blogspot.ch/2011/11/google-maps-query-string-parameters.html
	    String lat_and_long = null;
	    if ((lat_and_long = query_params.get("ll")) != null
		|| (lat_and_long = query_params.get("sll")) != null
		|| (lat_and_long = query_params.get("cbll")) != null
		|| (lat_and_long = query_params.get("q")) != null) {
		String[] coordinates = lat_and_long.split(",");
		double latitude = Double.parseDouble(coordinates[0]);
		double longitude = Double.parseDouble(coordinates[1]);
		location.addProperty("latitude", latitude);
		location.addProperty("longitude", longitude);
	    }
	    location.addProperty("location_text", loc.text());
	    status.add("location", location);
	}

	return status;
    }

    private static final String HTML_OPTION = "html";
    
    @SuppressWarnings("static-access")
    public static void main(String[] args) throws Exception {
	Options options = new Options();
	options.addOption(OptionBuilder.withArgName("path").hasArg()
			  .withDescription("HTML file from twitter.com").create(HTML_OPTION));
	
	CommandLine cmdline = null;
	CommandLineParser parser = new GnuParser();
	try {
	    cmdline = parser.parse(options, args);
	} catch (ParseException exp) {
	    System.err.println("Error parsing command line: " + exp.getMessage());
	    System.exit(-1);
	}
	
	if (!cmdline.hasOption(HTML_OPTION)) {
	    HelpFormatter formatter = new HelpFormatter();
	    formatter.printHelp(HTMLStatusExtractor.class.getName(), options);
	    System.exit(-1);
	}
	
	String html_filename = cmdline.getOptionValue(HTML_OPTION);
	BufferedReader html_file = null;
	StringBuffer buf = new StringBuffer();
	try {
	    html_file = new BufferedReader(new InputStreamReader(new FileInputStream(html_filename)));
	    String line;
	    while ((line = html_file.readLine()) != null) {
		buf.append(line);
		buf.append('\n');
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	} finally {
	    html_file.close();
	}

	HTMLStatusExtractor hse = new HTMLStatusExtractor();
	JsonObject json = hse.extractTweet(buf.toString());
	Gson gson = new GsonBuilder().setPrettyPrinting().create();
	System.out.println(gson.toJson(json));
    }
}
