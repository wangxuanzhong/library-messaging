package com.syswin.library.messaging.rocketmqons;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.aliyun.openservices.ons.api.PropertyValueConst;
import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqConsumer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

@Slf4j
public class ProducerAndConsumerTest {

  protected static final String topic = "simpleTopic";
  protected static final String orderTopic = "PartitionOrderTopic";
  protected static final String tag = "*";
  protected static final String message1 = "message1";
  protected static final String message2 = "message2";
  protected static final String message3 = "message3";
  private static final String NAMESRV_ADDR = "localhost:9876";
  private static final String ACCESS_KEY = "access_key";
  private static final String SECRET_KEY = "secret_key";
  private static final String GROUP_ID = "GID-temail-test";
  protected static final RocketMqOnsConfig mqConfig = new RocketMqOnsConfig(NAMESRV_ADDR, ACCESS_KEY, SECRET_KEY, GROUP_ID);

  Queue<String> messagesQueue = new ConcurrentLinkedQueue<>();

  private MqConsumer mqConsumer;
  private RocketMqOnsProducer mqProducer;

  @BeforeClass
  public static void init() {
    System.setProperty("rocketmq.client.rebalance.lockInterval", "1000");
  }

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
    mqConsumer = new RocketMqOnsConsumer(mqConfig, topic, tag, PropertyValueConst.CLUSTERING,
        messagesQueue::add);
    mqConsumer.start();

    mqProducer = new RocketMqOnsProducer(mqConfig);
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

  @Test
  public void sendOrderly() throws MessagingException {
    long executingTime = System.currentTimeMillis();
    try {
      mqConsumer = new OrderedRocketMqOnsConsumer(mqConfig, orderTopic, "", PropertyValueConst.CLUSTERING,
          messagesQueue::add);
      mqConsumer.start();
      mqProducer = new RocketMqOnsProducer(mqConfig);
      mqProducer.start();
      mqProducer.sendOrderly(message1 + executingTime, orderTopic);
      mqProducer.sendOrderly(message2 + executingTime, orderTopic);
      mqProducer.sendOrderly(message3 + executingTime, orderTopic);
      await().atMost(50, SECONDS).untilAsserted(() -> assertThat(messagesQueue).size().isGreaterThan(0));
      assertThat(messagesQueue).hasSize(3);
      assertThat(messagesQueue)
          .containsExactly(message1 + executingTime, message2 + executingTime, message3 + executingTime);
    } finally {
      log.info("执行时间：" + (System.currentTimeMillis() - executingTime) + "毫秒！");
    }
  }

}
