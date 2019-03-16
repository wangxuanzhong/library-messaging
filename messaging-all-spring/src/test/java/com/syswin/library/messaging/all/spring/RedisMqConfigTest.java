package com.syswin.library.messaging.all.spring;

import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.all.spring.containers.RedisContainer;
import com.syswin.library.messaging.redis.spring.RedisMqConsumer;
import com.syswin.library.messaging.redis.spring.RedisMqProducer;
import com.syswin.library.messaging.test.spring.MqConfigTestApp;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
    "library.messaging.type=redis",
    "app.producer.group=producer",
    "app.consumer.group=consumer",
    "app.consumer.topic=" + MqConfigTestBase.TOPIC,
    "app.consumer.tag=*"
}, classes = MqConfigTestApp.class)
public class RedisMqConfigTest extends MqConfigTestBase {

  @ClassRule
  public static final RedisContainer redis = new RedisContainer();

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("spring.redis.host", redis.getContainerIpAddress());
    System.setProperty("spring.redis.port", String.valueOf(redis.getMappedPort(6379)));
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("spring.redis.host");
    System.clearProperty("spring.redis.port");
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
