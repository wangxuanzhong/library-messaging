package com.syswin.library.messaging.all.spring;

import static com.seanyinx.github.unit.scaffolding.Randomness.uniquify;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.waitAtMost;

import com.syswin.library.messaging.all.spring.containers.RocketMqBrokerContainer;
import com.syswin.library.messaging.all.spring.containers.RocketMqNameServerContainer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.testcontainers.containers.Network;

public class RocketMqConfigTest extends MqConfigTestBase {
  private static final String hostname = "namesrv";
  private static final int MQ_SERVER_PORT = 9876;

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

  private static final DefaultMQProducer mqProducer = new DefaultMQProducer(uniquify("test-producer-group"));

  private static String brokerAddress;

  @ClassRule
  public static RuleChain rules = RuleChain.outerRule(rocketMqNameSrv).around(rocketMqBroker);

  @BeforeClass
  public static void beforeClass() throws MQClientException {
    brokerAddress = rocketMqNameSrv.getContainerIpAddress() + ":" + MQ_SERVER_PORT;

    System.setProperty("library.messaging.type", "rocketmq");
    System.setProperty("library.messaging.rocketmq.broker.address", brokerAddress);

    createMqTopic();
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("library.messaging.rocketmq.broker.address");
    System.clearProperty("library.messaging.type");
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
}
