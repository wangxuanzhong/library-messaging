/*
 * MIT License
 *
 * Copyright (c) 2019 Syswin
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.syswin.library.messaging.rocketmqons;

import static com.seanyinx.github.unit.scaffolding.Randomness.uniquify;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;

import com.aliyun.openservices.ons.api.PropertyValueConst;
import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqConsumer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.testcontainers.containers.Network;

@Slf4j
public class ProducerAndConsumerTest {

  private static final String topic = "simpleTopic";
  private static final String orderTopic = "PartitionOrderTopic";
  private static final String tag = "*";
  private static final String message1 = "message1";
  private static final String message2 = "message2";
  private static final String message3 = "message3";

  private static final String ACCESS_KEY = uniquify("access_key");
  private static final String SECRET_KEY = uniquify("secret_key");
  private static final String GROUP_ID = "GID-temail-test";

  private static final Network NETWORK = Network.newNetwork();
  private static final String hostname = "namesrv";
  private static final int MQ_SERVER_PORT = 9876;

  private static final RocketMqNameServerContainer rocketMqNameSrv = new RocketMqNameServerContainer()
      .withNetwork(NETWORK)
      .withNetworkAliases(hostname)
      .withFixedExposedPort(MQ_SERVER_PORT, MQ_SERVER_PORT);

  private static final RocketMqBrokerContainer rocketMqBroker = new RocketMqBrokerContainer()
      .withNetwork(NETWORK)
      .withEnv("NAMESRV_ADDR", hostname + ":" + MQ_SERVER_PORT)
      .withFixedExposedPort(10909, 10909)
      .withFixedExposedPort(10911, 10911);

  private static final DefaultMQProducer producer = new DefaultMQProducer(uniquify("test-producer-group"));

  @ClassRule
  public static final RuleChain rules = RuleChain.outerRule(rocketMqNameSrv).around(rocketMqBroker);

  private static final RocketMqOnsConfig mqConfig = new RocketMqOnsConfig(rocketMqNameSrv.getContainerIpAddress() + ":" + MQ_SERVER_PORT, ACCESS_KEY, SECRET_KEY, GROUP_ID);

  private final Queue<String> messagesQueue = new ConcurrentLinkedQueue<>();

  private MqConsumer mqConsumer;
  private RocketMqOnsProducer mqProducer;

  @BeforeClass
  public static void init() throws MQClientException {
    System.setProperty("rocketmq.client.rebalance.lockInterval", "1000");
    createMqTopic(topic, orderTopic);
  }

  private static void createMqTopic(String... topics) throws MQClientException {
    producer.setNamesrvAddr(rocketMqNameSrv.getContainerIpAddress() + ":" + MQ_SERVER_PORT);
    producer.start();

    // ensure topic exists before consumer connects, or no message will be received
    waitAtMost(10, SECONDS).until(() -> {
      try {
        for (String topic : topics) {
          producer.createTopic(producer.getCreateTopicKey(), topic, 4);
        }
        return true;
      } catch (MQClientException e) {
        e.printStackTrace();
        return false;
      }
    });
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
