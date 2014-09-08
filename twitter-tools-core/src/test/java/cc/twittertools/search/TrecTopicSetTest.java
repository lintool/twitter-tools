package cc.twittertools.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import com.google.common.collect.Lists;

public class TrecTopicSetTest {

  @Test
  public void topics2011() throws Exception {
    File f = new File("../data/topics.microblog2011.txt");
    assertTrue(f.exists());

    TrecTopicSet topics = TrecTopicSet.fromFile(f);
    List<TrecTopic> t = Lists.newArrayList(topics.iterator());

    assertEquals(50, t.size());
    assertEquals("MB01", t.get(0).getId());
    assertEquals("MB50", t.get(t.size()-1).getId());
  }

  @Test
  public void topics2012() throws Exception {
    File f = new File("../data/topics.microblog2012.txt");
    assertTrue(f.exists());

    TrecTopicSet topics = TrecTopicSet.fromFile(f);
    List<TrecTopic> t = Lists.newArrayList(topics.iterator());

    assertEquals(60, t.size());
    assertEquals("MB51", t.get(0).getId());
    assertEquals("MB110", t.get(t.size()-1).getId());
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(TrecTopicSetTest.class);
  }
}
