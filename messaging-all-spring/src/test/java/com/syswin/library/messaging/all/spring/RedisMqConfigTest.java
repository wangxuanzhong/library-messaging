package com.syswin.library.messaging.all.spring;

import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.all.spring.containers.RedisContainer;
import com.syswin.library.messaging.redis.spring.RedisMqConsumer;
import com.syswin.library.messaging.redis.spring.RedisMqProducer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

public class RedisMqConfigTest extends MqConfigTestBase {

  @ClassRule
  public static final RedisContainer redis = new RedisContainer();

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("library.messaging.type", "redis");
    System.setProperty("spring.redis.host", redis.getContainerIpAddress());
    System.setProperty("spring.redis.port", String.valueOf(redis.getMappedPort(6379)));
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("spring.redis.host");
    System.clearProperty("spring.redis.port");
    System.clearProperty("library.messaging.type");
  }

  @Override
  Class<? extends MqConsumer> consumerType() {
    return RedisMqConsumer.class;
  }

  @Override
  Class<? extends MqProducer> producerType() {
    return RedisMqProducer.class;
  }
}
