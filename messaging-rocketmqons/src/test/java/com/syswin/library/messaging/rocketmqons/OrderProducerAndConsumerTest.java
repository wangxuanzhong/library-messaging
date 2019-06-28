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

public class OrderProducerAndConsumerTest extends AbstractRocketMqTest {

  Queue<String> messagesQueue = new ConcurrentLinkedQueue<>();
  public static final String ORDER_GROUP_ID = "GID-order-test";
  public static final String TAG = "mq_test_tag";

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
    long executingTime = System.currentTimeMillis();
    try {
      System.setProperty("rocketmq.client.rebalance.lockInterval", "1000");
      mqConsumer = new OrderedRocketMqConsumer(mqConfig, orderTopic, "", PropertyValueConst.CLUSTERING,
          messagesQueue::add);
      mqConsumer.start();
      mqProducer = new OrderedRocketMqProducer(mqConfig);
      mqProducer.start();
      mqProducer.sendOrderly(message1 + executingTime, orderTopic);
      mqProducer.sendOrderly(message2 + executingTime, orderTopic);
      mqProducer.sendOrderly(message3 + executingTime, orderTopic);
      await().atMost(50, SECONDS).untilAsserted(() -> assertThat(messagesQueue).size().isGreaterThan(0));
      assertThat(messagesQueue).hasSize(3);
      assertThat(messagesQueue)
          .containsExactly(message1 + executingTime, message2 + executingTime, message3 + executingTime);
    } finally {
      System.out.println("执行时间：" + (System.currentTimeMillis() - executingTime) + "毫秒！");
    }
  }


  @Test(expected = UnsupportedOperationException.class)
  public void sendRandomly() throws MessagingException, UnsupportedEncodingException, InterruptedException {
    mqProducer = new OrderedRocketMqProducer(mqConfig);
    mqProducer.start();
    mqProducer.sendRandomly(message1, topic);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void send() throws MessagingException, UnsupportedEncodingException, InterruptedException {
    mqProducer = new OrderedRocketMqProducer(mqConfig);
    mqProducer.start();
    mqProducer.send(message1, topic, "", message1);
  }

}
