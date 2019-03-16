package com.syswin.library.messaging.all.spring;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.test.spring.MqConfigTestApp;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {
    "app.producer.group=producer",
    "app.consumer.group=consumer",
    "app.consumer.topic=" + EmbeddedMqConfigTest.TOPIC,
    "app.consumer.tag=*"
}, classes = MqConfigTestApp.class)
public abstract class MqConfigTestBase {

  static final String TOPIC = "brave-new-world";
  private static final String MESSAGE = "hello";

  @Autowired
  private Map<String, MqProducer> producers;

  @Autowired
  private List<? extends MqConsumer> consumers;

  @Autowired
  private Queue<String> messages;

  @Test
  public void shouldReceiveSentMessages() throws MessagingException, UnsupportedEncodingException, InterruptedException {
    MqProducer producer = producers.get("producer");
    producer.sendOrderly(MESSAGE, TOPIC);

    await().atMost(1, SECONDS).untilAsserted(() -> assertThat(messages).hasSize(1));

    assertThat(messages).containsExactlyInAnyOrder(MESSAGE);
    producers.values().forEach(p -> assertThat(p).isInstanceOf(producerType()));
    consumers.forEach(c -> assertThat(c).isInstanceOf(consumerType()));
  }

  abstract Class<? extends MqConsumer> consumerType();

  abstract Class<? extends MqProducer> producerType();
}
