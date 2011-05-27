package com.twitter.corpus.index;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class TokenizationTest {
  @Test
  public void basic() throws Exception {
    assertTrue(Character.isLetter((int) 'j'));

    assertFalse(Character.isLetter((int) '5'));
    assertTrue(Character.isDigit((int) '5'));
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(TokenizationTest.class);
  }
}
