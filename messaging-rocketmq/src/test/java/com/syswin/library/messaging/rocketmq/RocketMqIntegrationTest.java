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

package com.syswin.library.messaging.rocketmq;

import static com.seanyinx.github.unit.scaffolding.AssertUtils.expectFailing;
import static com.seanyinx.github.unit.scaffolding.Randomness.uniquify;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.CLUSTERING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;

import com.syswin.library.messaging.MessageClientException;
import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.rocketmq.containers.RocketMqBrokerContainer;
import com.syswin.library.messaging.rocketmq.containers.RocketMqNameServerContainer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.testcontainers.containers.Network;

public class RocketMqIntegrationTest {

  private static final String hostname = "namesrv";
  private static final int MQ_SERVER_PORT = 9876;

  private static final DefaultMQProducer mqProducer = new DefaultMQProducer(uniquify("test-producer-group"));

  private static final Network NETWORK = Network.newNetwork();
  private static final RocketMqNameServerContainer rocketMqNameSrv = new RocketMqNameServerContainer()
      .withNetwork(NETWORK)
      .withNetworkAliases(hostname)
      .withFixedExposedPort(MQ_SERVER_PORT, MQ_SERVER_PORT);

  private static final RocketMqBrokerContainer rocketMqBroker = new RocketMqBrokerContainer()
      .withNetwork(NETWORK)
      .withEnv("NAMESRV_ADDR", hostname + ":" + MQ_SERVER_PORT)
      .withFixedExposedPort(10909, 10909)
      .withFixedExposedPort(10911, 10911);

  private static final String topic = "brave-new-world";
  private static final String tag = "*";
  private static final String keys = "";

  private static final String message1 = "message1";
  private static final String message2 = "message2";
  private static final String message3 = "message3";

  private static String brokerAddress;

  @ClassRule
  public static RuleChain rules = RuleChain.outerRule(rocketMqNameSrv).around(rocketMqBroker);

  private final Queue<String> messagesConcurrent = new ConcurrentLinkedQueue<>();
  private final Queue<String> messagesOrdered = new ConcurrentLinkedQueue<>();

  private MqProducer rocketMqProducer;
  private MqConsumer concurrentMqConsumer;
  private MqConsumer orderedMqConsumer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    brokerAddress = rocketMqNameSrv.getContainerIpAddress() + ":" + MQ_SERVER_PORT;
    createMqTopic();
  }

  @AfterClass
  public static void afterClass() {
    mqProducer.shutdown();
  }

  private static void createMqTopic() throws MQClientException {
    mqProducer.setNamesrvAddr(brokerAddress);
    mqProducer.start();

    // ensure topic exists before consumer connects, or no message will be received
    waitAtMost(10, SECONDS).until(() -> {
      try {
        mqProducer.createTopic(mqProducer.getCreateTopicKey(), topic, 4);
        return true;
      } catch (MQClientException e) {
        e.printStackTrace();
        return false;
      }
    });
  }

  @Before
  public void setUp() throws Exception {
    rocketMqProducer = new RocketMqProducer(brokerAddress, "data-consistency");
    rocketMqProducer.start();

    concurrentMqConsumer = new ConcurrentRocketMqConsumer(brokerAddress, "consumer-concurrent", topic, tag, CLUSTERING, messagesConcurrent::add);
    concurrentMqConsumer.start();

    orderedMqConsumer = new OrderedRocketMqConsumer(brokerAddress, "consumer-group-ordered", topic, tag, CLUSTERING, messagesOrdered::add);
    orderedMqConsumer.start();
  }

  @After
  public void tearDown() {
    rocketMqProducer.shutdown();
    concurrentMqConsumer.shutdown();
    orderedMqConsumer.shutdown();
  }

  @Test
  public void shouldReceiveSentMessages() throws Exception {
    rocketMqProducer.send(message1, topic, tag, keys);
    rocketMqProducer.sendOrderly(message2, topic);
    rocketMqProducer.sendRandomly(message3, topic);

    await().atMost(5, SECONDS).untilAsserted(() -> assertThat(messagesConcurrent).hasSize(3));
    await().atMost(5, SECONDS).untilAsserted(() -> assertThat(messagesOrdered).hasSize(3));

    assertThat(messagesConcurrent).containsExactlyInAnyOrder(message1, message2, message3);
    assertThat(messagesOrdered).containsExactlyInAnyOrder(message1, message2, message3);

    rocketMqProducer.shutdown();
    try {
      rocketMqProducer.sendRandomly(message1, topic);
      expectFailing(MessageClientException.class);
    } catch (MessageClientException ignored) {
    }
  }
}
