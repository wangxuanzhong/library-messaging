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

    await().atMost(1, SECONDS).untilAsserted(() -> assertThat(messages).hasSize(3));

    assertThat(messages).containsAll(sentMessages);
  }
}
