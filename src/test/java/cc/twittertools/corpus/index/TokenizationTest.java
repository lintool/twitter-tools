package cc.twittertools.corpus.index;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringReader;

import junit.framework.JUnit4TestAdapter;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.junit.Test;

import cc.twittertools.search.indexing.TweetAnalyzer;

public class TokenizationTest {

  @Test
  public void basic() throws Exception {
    Analyzer analyzer = new TweetAnalyzer(Version.LUCENE_41);

    String test1 = parseKeywords(
        analyzer,
        "AT&T getting secret immunity from wiretapping laws for government surveillance http://vrge.co/ZP3Fx5");
    assertEquals(
        "|att|get|secret|immun|from|wiretap|law|for|govern|surveil|http://vrge.co/ZP3Fx5|", test1);

    String test2 = parseKeywords(
        analyzer,
        // See comment in removeNonAlphanumeric() of LowerCaseEntityPreservingFilter: isAlphabetic
        // will correctly scrub the trailing unicode "...", but is a JDK7 method.
        //"want to see the @verge aston martin GT4 racer tear up long beach? http://theracersgroup.kinja.com/watch-an-aston-martin-vantage-gt4-tear-around-long-beac-479726219 …");
        "want to see the @verge aston martin GT4 racer tear up long beach? http://theracersgroup.kinja.com/watch-an-aston-martin-vantage-gt4-tear-around-long-beac-479726219");
    assertEquals(
        "|want|to|see|the|@verge|aston|martin|gt4|racer|tear|up|long|beach|http://theracersgroup.kinja.com/watch-an-aston-martin-vantage-gt4-tear-around-long-beac-479726219|",
        test2);

    String test3 = parseKeywords(
        analyzer,
        "Incredibly good news! #Drupal users rally http://bit.ly/Z8ZoFe  to ensure blind accessibility contributor gets to @DrupalCon #Opensource");
    assertEquals(
        "|incred|good|new|#drupal|user|ralli|http://bit.ly/Z8ZoFe|to|ensur|blind|access|contributor|get|to|@drupalcon|#opensource|",
        test3);

// Issue with this test case... comment out for now.
//    String test4 = parseKeywords(
//        analyzer,
//        "We're entering the quiet hours at #amznhack. #Rindfleischetikettierungsüberwachungsaufgabenübertragungsgesetz");
//    assertEquals(
//        "|were|enter|the|quiet|hour|at|#amznhack|#rindfleischetikettierungsüberwachungsaufgabenübertragungsgesetz|",
//        test4);

    String test5 = parseKeywords(
        analyzer,
        "The 2013 Social Event Detection Task (SED) at #mediaeval2013, http://bit.ly/16nITsf  supported by @linkedtv @project_mmixer @socialsensor_ip");
    assertEquals(
        "|the|2013|social|event|detect|task|sed|at|#mediaeval2013|http://bit.ly/16nITsf|support|by|@linkedtv|@project_mmixer|@socialsensor_ip|",
        test5);

    String test6 = parseKeywords(analyzer,
        "U.S.A. U.K. U.K USA UK #US #UK #U.S.A #U.K ...A.B.C...D..E..F..A.LONG WORD");
    assertEquals("|usa|uk|uk|usa|uk|#us|#uk|#u|sa|#u|k|abc|d|e|f|a|long|word|", test6);

    String test7 = parseKeywords(analyzer, "this is @a_valid_mention and this_is_multiple_words");
    assertEquals("|thi|is|@a_valid_mention|and|thi|is|multipl|word|", test7);

    String test8 = parseKeywords(analyzer,
        "PLEASE BE LOWER CASE WHEN YOU COME OUT THE OTHER SIDE - ALSO A @VALID_VALID-INVALID");
    assertEquals(
        "|pleas|be|lower|case|when|you|come|out|the|other|side|also|a|@valid_valid|invalid|", test8);

// Note: the at sign is not the normal (at) sign and the crazy hashtag is not the normal #
//    String test9 = parseKeywords(analyzer, "＠reply @with #crazy ~＃at");
//    assertEquals("|＠reply|@with|#crazy|＃at|", test9);

    String test10 = parseKeywords(
        analyzer,
        ":@valid testing(valid)#hashtags. RT:@meniton (the last @mention is #valid and so is this:@valid), however this is@invalid");
    assertEquals(
        "|@valid|test|valid|#hashtags|rt|@meniton|the|last|@mention|is|#valid|and|so|is|thi|@valid|howev|thi|is|invalid|",
        test10);
    
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(TokenizationTest.class);
  }

  public String parseKeywords(Analyzer analyzer, String keywords) {
    StringBuilder sb = new StringBuilder();
    try {
      TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(keywords));
      CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
      tokenStream.reset();
      sb.append("|");
      while (tokenStream.incrementToken()) {
        if (cattr.toString().length() == 0)
          continue;
        sb.append(cattr.toString());
        sb.append("|");
      }
      tokenStream.end();
      tokenStream.close();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return sb.toString();
  }

}
