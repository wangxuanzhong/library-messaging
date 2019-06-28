package com.syswin.library.messaging.rocketmqons;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.aliyun.openservices.ons.api.PropertyValueConst;
import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.MqProducer;
import java.io.UnsupportedEncodingException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.After;
import org.junit.Test;

public class ProducerAndConsumerTest extends AbstractRocketMqTest {

  Queue<String> messagesQueue = new ConcurrentLinkedQueue<>();

  private MqConsumer mqConsumer;
  private MqProducer mqProducer;

  @After
  public void tearDown() {
    messagesQueue.clear();

    if (mqProducer != null) {
      mqProducer.shutdown();
    }
    if (mqConsumer != null) {
      mqConsumer.shutdown();
    }
  }

  @Test
  public void testSendAndReceive() throws Exception {
    long executeTime = System.currentTimeMillis();
    mqConsumer = new RocketMqConsumer(mqConfig, topic, tag, PropertyValueConst.CLUSTERING,
        messagesQueue::add);
    mqConsumer.start();

    mqProducer = new RocketMqProducer(mqConfig);
    mqProducer.start();

    mqProducer.send(message1 + executeTime, topic, "TestTag", message1);
    mqProducer.send(message3 + executeTime, topic, "TestTag", message3);
    mqProducer.sendRandomly(message2 + executeTime, topic);
    mqProducer.sendRandomly(message3 + executeTime, topic);
    await().atMost(10, SECONDS).untilAsserted(() -> assertThat(messagesQueue).hasSize(4));
    assertThat(messagesQueue)
        .containsExactlyInAnyOrder(message1 + executeTime, message2 + executeTime, message3 + executeTime,
            message3 + executeTime);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void sendOrderly() throws MessagingException, UnsupportedEncodingException, InterruptedException {
    mqProducer = new RocketMqProducer(mqConfig);
    mqProducer.start();
    mqProducer.sendOrderly(message1, topic);
  }

}
