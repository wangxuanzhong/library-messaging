package com.syswin.library.messaging.all.spring;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.rocketmqons.RocketMqOnsProducer;
import com.syswin.library.messaging.test.mixed.RocketMqOnsConfigTestConfiguration;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {
    "library.messaging.redis.enabled=false",
    "library.messaging.embedded.enabled=false",
    "library.messaging.rocketmq.enabled=false",
    "library.messaging.rocketmqons.enabled=true",
}, classes = RocketMqOnsConfigTestConfiguration.class)
public class RocketMqOnsConfigTest {
  private static final String NAMESRV_ADDR = "localhost:9876";
  private static final String ACCESS_KEY = "access_key";
  private static final String SECRET_KEY = "secret_key";
  private static final String GROUP_ID = "GID-temail-test";


  @BeforeClass
  public static void beforeClass() {
    System.setProperty("spring.rocketmqons.host", NAMESRV_ADDR);
    System.setProperty("spring.rocketmqons.accessKey", ACCESS_KEY);
    System.setProperty("spring.rocketmqons.secretKey", SECRET_KEY);
    System.setProperty("rocketmq.client.rebalance.lockInterval", "1000");
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("spring.rocketmqons.host");
    System.clearProperty("spring.rocketmqons.accessKey");
    System.clearProperty("spring.rocketmqons.secretKey");
  }

  private static final String TOPIC = "PartitionOrderTopic";
  private static final String MESSAGE = "hello";

  @Autowired
  private Map<String, MqProducer> producers;

  @Autowired
  private List<MqConsumer> consumers;

  @Autowired
  private Queue<String> messages;

  @Test
  public void shouldReceiveSentMessages()
      throws MessagingException, UnsupportedEncodingException, InterruptedException {
    List<String> sentMessages = new ArrayList<>(producers.size());
    for (String group : producers.keySet()) {
      String message = group + MESSAGE;
      producers.get(group).sendOrderly(message, TOPIC);
      sentMessages.add(message);
    }

    await().atMost(50, SECONDS).untilAsserted(() -> assertThat(messages).size().isGreaterThan(0));
    assertThat(messages).hasSize(sentMessages.size());
    assertThat(messages).containsAll(sentMessages);
    assertThat(producers.get(GROUP_ID)).isInstanceOf(RocketMqOnsProducer.class);
  }
}
