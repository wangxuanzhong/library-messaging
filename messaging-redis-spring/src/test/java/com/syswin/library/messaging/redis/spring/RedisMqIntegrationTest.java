package com.syswin.library.messaging.redis.spring;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.redis.spring.containers.RedisContainer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {RedisTestApp.class})
public class RedisMqIntegrationTest {

  private static final String topic = "brave-new-world";
  private static final String tag = "*";
  private static final String keys = "";

  private static final String message1 = "message1";
  private static final String message2 = "message2";
  private static final String message3 = "message3";

  @ClassRule
  public static final RedisContainer redis = new RedisContainer();

  private static final Queue<String> messages = new ConcurrentLinkedQueue<>();
  private final ExecutorService executor = Executors.newFixedThreadPool(2);
  private final RedisMessageListenerContainer container = new RedisMessageListenerContainer();

  @Autowired
  private RedisConnectionFactory connectionFactory;

  @Autowired
  private RedisTemplate<String, Object> redisTemplate;

  private MqProducer mqProducer;
  private static RedisMqConsumer mqConsumer;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("spring.redis.host", redis.getContainerIpAddress());
    System.setProperty("spring.redis.port", String.valueOf(redis.getMappedPort(6379)));


    mqConsumer = new RedisMqConsumer(topic, new ExceptionThrowingConsumer(messages));
    mqConsumer.start();
  }

  @AfterClass
  public static void afterClass() {
    mqConsumer.shutdown();

    System.clearProperty("spring.redis.host");
    System.clearProperty("spring.redis.port");
  }

  @Before
  public void setUp() throws Exception {
    mqProducer = new RedisMqProducer(redisTemplate);
    mqProducer.start();

    container.setConnectionFactory(connectionFactory);
    container.addMessageListener(mqConsumer, new ChannelTopic(mqConsumer.topic()));
    container.setSubscriptionExecutor(executor);
    container.setTaskExecutor(executor);
    container.start();
  }

  @After
  public void tearDown() throws Exception {
    mqProducer.shutdown();
    container.destroy();
  }

  @Test
  public void shouldReceiveSentMessages() throws Exception {
    mqProducer.send(message2, topic, tag, keys);
    mqProducer.send(message1, topic, tag, keys);
    mqProducer.sendOrderly(message2, topic);
    mqProducer.sendRandomly(message3, topic);

    await().atMost(1, SECONDS).untilAsserted(() -> assertThat(messages).hasSize(3));

    assertThat(messages).containsExactlyInAnyOrder(message1, message2, message3);
  }

  private static class ExceptionThrowingConsumer implements Consumer<String> {

    private final Queue<String> messages;
    private final AtomicBoolean failed = new AtomicBoolean(true);

    ExceptionThrowingConsumer(Queue<String> messages) {
      this.messages = messages;
    }

    @Override
    public void accept(String message) {
      if (failed.compareAndSet(true, false)) {
        throw new IllegalStateException("oops");
      }
      messages.add(message);
    }
  }
}
