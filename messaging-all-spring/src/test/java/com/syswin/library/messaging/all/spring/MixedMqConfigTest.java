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

package com.syswin.library.messaging.all.spring;

import static com.seanyinx.github.unit.scaffolding.Randomness.uniquify;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;

import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.all.spring.containers.RedisContainer;
import com.syswin.library.messaging.all.spring.containers.RocketMqBrokerContainer;
import com.syswin.library.messaging.all.spring.containers.RocketMqNameServerContainer;
import com.syswin.library.messaging.embedded.EmbeddedMqProducer;
import com.syswin.library.messaging.redis.spring.RedisMqProducer;
import com.syswin.library.messaging.redis.spring.RedisPersistentMqProducer;
import com.syswin.library.messaging.rocketmq.RocketMqProducer;
import com.syswin.library.messaging.test.mixed.MixedMqConfigTestApp;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.Network;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {
    "library.messaging.redis.enabled=true",
    "library.messaging.redis-persistence.enabled=true",
    "library.messaging.embedded.enabled=true",
    "library.messaging.rocketmq.enabled=true",
}, classes = MixedMqConfigTestApp.class)
public class MixedMqConfigTest {

  private static final String hostname = "namesrv";
  private static final int MQ_SERVER_PORT = 9876;

  private static final Network NETWORK = Network.newNetwork();
  private static final RedisContainer redis = new RedisContainer().withNetwork(NETWORK);

  private static final RocketMqNameServerContainer rocketMqNameSrv = new RocketMqNameServerContainer()
      .withNetwork(NETWORK)
      .withNetworkAliases(hostname)
      .withFixedExposedPort(MQ_SERVER_PORT, MQ_SERVER_PORT);

  private static final RocketMqBrokerContainer rocketMqBroker = new RocketMqBrokerContainer()
      .withNetwork(NETWORK)
      .withEnv("NAMESRV_ADDR", hostname + ":" + MQ_SERVER_PORT)
      .withFixedExposedPort(10909, 10909)
      .withFixedExposedPort(10911, 10911);

  private static final DefaultMQProducer mqProducer = new DefaultMQProducer(uniquify("test-producer-group"));

  private static String brokerAddress;

  @ClassRule
  public static RuleChain rules = RuleChain.outerRule(rocketMqNameSrv).around(rocketMqBroker).around(redis);

  @BeforeClass
  public static void beforeClass() throws MQClientException {
    brokerAddress = rocketMqNameSrv.getContainerIpAddress() + ":" + MQ_SERVER_PORT;

    System.setProperty("spring.redis.host", redis.getContainerIpAddress());
    System.setProperty("spring.redis.port", String.valueOf(redis.getMappedPort(6379)));
    System.setProperty("spring.rocketmq.host", brokerAddress);

    createMqTopic();
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("spring.rocketmq.host");
    System.clearProperty("spring.redis.host");
    System.clearProperty("spring.redis.port");
  }

  private static void createMqTopic() throws MQClientException {
    mqProducer.setNamesrvAddr(brokerAddress);
    mqProducer.start();

    // ensure topic exists before consumer connects, or no message will be received
    waitAtMost(10, SECONDS).until(() -> {
      try {
        mqProducer.createTopic(mqProducer.getCreateTopicKey(), TOPIC, 4);
        return true;
      } catch (MQClientException e) {
        e.printStackTrace();
        return false;
      }
    });
  }

  private static final String TOPIC = "brave-new-world";
  private static final String MESSAGE = "hello";

  @Autowired
  private Map<String, MqProducer> producers;

  @Autowired
  private List<MqConsumer> consumers;

  @Autowired
  private Queue<String> messages;

  @Test
  public void shouldReceiveSentMessages() throws MessagingException, UnsupportedEncodingException, InterruptedException {
    List<String> sentMessages = new ArrayList<>(producers.size());
    for (String group : producers.keySet()) {
      String message = group + MESSAGE;
      producers.get(group).sendOrderly(message, TOPIC);
      sentMessages.add(message);
    }

    await().atMost(1, SECONDS).untilAsserted(() -> assertThat(messages).hasSize(4));

    assertThat(messages).containsAll(sentMessages);

    assertThat(producers.get("redis")).isInstanceOf(RedisMqProducer.class);
    assertThat(producers.get("redis-persistence")).isInstanceOf(RedisPersistentMqProducer.class);
    assertThat(producers.get("rocketmq")).isInstanceOf(RocketMqProducer.class);
    assertThat(producers.get("embedded")).isInstanceOf(EmbeddedMqProducer.class);
  }
}
