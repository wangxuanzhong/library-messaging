package com.syswin.library.messaging.all.spring;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class EmbeddedMqConfigTest extends MqConfigTestBase {

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("library.messaging.type", "embedded");
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("library.messaging.type");
  }
}
