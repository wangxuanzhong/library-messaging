package com.syswin.library.messaging.all.spring;

import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.embedded.EmbeddedMqConsumer;
import com.syswin.library.messaging.embedded.EmbeddedMqProducer;
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

  @Override
  Class<? extends MqConsumer> consumerType() {
    return EmbeddedMqConsumer.class;
  }

  @Override
  Class<? extends MqProducer> producerType() {
    return EmbeddedMqProducer.class;
  }
}
